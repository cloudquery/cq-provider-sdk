package providertest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/cloudquery/cq-provider-sdk/logging"
	"github.com/cloudquery/cq-provider-sdk/provider"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/go-test/deep"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/hashicorp/terraform-exec/tfexec"
	"github.com/jackc/pgx/v4"
	"github.com/tmccombs/hcl2json/convert"
)

const (
	tfDir    = "./int_test/tf/"
	tfOrigin = "./testData/"
)

type ResourceIntegrationTestData struct {
	Table               *schema.Table
	Config              interface{}
	Resources           []string
	Configure           func(logger hclog.Logger, data interface{}) (schema.ClientMeta, error)
	Suffix              string
	Prefix              string
	VerificationBuilder func(res *ResourceIntegrationTestData) ResourceIntegrationVerification
}

// ResourceIntegrationVerification - a set of verification rules to query and check test related data
type ResourceIntegrationVerification struct {
	Name           string
	ForeignKeyName string
	ExpectedValues []ExpectedValue
	Filter         func(sq squirrel.SelectBuilder, res *ResourceIntegrationTestData) squirrel.SelectBuilder
	Relations      []*ResourceIntegrationVerification
}

// ExpectedValue - describes the data that expected to be in database after fetch
type ExpectedValue struct {
	Count int                    // expected count of items
	Data  map[string]interface{} // expected data of items
}

// IntegrationTest - creates resources using terraform, fetches them to db and compares with expected values
func IntegrationTest(t *testing.T, providerCreator func() *provider.Provider, resource ResourceIntegrationTestData) {
	t.Parallel()
	workdir, err := copyTfFiles(resource.Table.Name)
	if err != nil {
		t.Fatal(err)
	}
	lookPath := os.Getenv("TF_EXEC_PATH")
	if lookPath == "" {
		lookPath = "terraform"
	}
	execPath, err := exec.LookPath(lookPath)
	if err != nil {
		t.Fatal(err)
	}
	tf, err := tfexec.NewTerraform(workdir, execPath)
	if err != nil {
		t.Fatal(err)
	}
	err = enableTerraformLog(tf, workdir)
	if err != nil {
		t.Fatal(err)
	}

	// prepare terraform variables
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	resource.Suffix = simplifyString(hostname)
	resource.Prefix = simplifyString(resource.Table.Name)
	testSuffix := fmt.Sprintf("test_suffix=%s", resource.Suffix)
	testPrefix := fmt.Sprintf("test_prefix=%s", resource.Prefix)

	ctx := context.Background()
	log.Printf("%s tf init\n", resource.Table.Name)
	err = tf.Init(ctx, tfexec.Upgrade(true))
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("%s tf apply\n", resource.Table.Name)
	err = tf.Apply(ctx, tfexec.Var(testPrefix), tfexec.Var(testSuffix))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		log.Printf("%s tf destroy\n", resource.Table.Name)
		err = tf.Destroy(ctx, tfexec.Var(testPrefix), tfexec.Var(testSuffix))
		if err != nil {
			t.Fatal(err)
		}
		err = os.RemoveAll(workdir)
		if err != nil {
			t.Fatal(err)
		}
		log.Printf("%s done\n", resource.Table.Name)
	}()

	log.Printf("%s fetch resources\n", resource.Table.Name)
	testProvider := providerCreator()
	testProvider.Logger = logging.New(hclog.DefaultOptions)

	// generate a config for provider
	f := hclwrite.NewFile()
	f.Body().AppendBlock(gohcl.EncodeAsBlock(resource.Config, "configuration"))
	data, err := convert.Bytes(f.Bytes(), "config.json", convert.Options{})
	hack := map[string]interface{}{}
	_ = json.Unmarshal(data, &hack)
	data, _ = json.Marshal(hack["configuration"].([]interface{})[0])
	if err != nil {
		t.Fatal(err)
	}

	testProvider.Configure = resource.Configure
	_, err = testProvider.ConfigureProvider(context.Background(), &cqproto.ConfigureProviderRequest{
		CloudQueryVersion: "",
		Connection:        cqproto.ConnectionDetails{DSN: "host=localhost user=postgres password=pass DB.hostname=postgres port=5432"},
		Config:            data,
	})

	if err != nil {
		t.Fatal(err)
	}

	_ = testProvider.FetchResources(context.Background(), &cqproto.FetchResourcesRequest{Resources: []string{findResourceFromTableName(resource.Table, testProvider.ResourceMap)}}, fakeResourceSender{})
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("%s verify fields\n", resource.Table.Name)
	conn, err := setupDatabase()
	if err != nil {
		t.Fatal(err)
	}
	err = verifyFields(t, resource, conn)
	if err != nil {
		t.Fatal(err)
	}
}

// enableTerraformLog - sets the path for terraform log files for current test
func enableTerraformLog(tf *tfexec.Terraform, workdir string) error {
	abs, err := filepath.Abs(workdir)
	if err != nil {
		return err
	}
	dst := abs + string(os.PathSeparator) + "tflog"

	if _, err = os.Create(dst); err != nil {
		return err
	}
	if err = tf.SetLogPath(dst); err != nil {
		return err
	}
	return nil
}

// verifyFields - gets the root db entry and check all its expected relations
func verifyFields(t *testing.T, resource ResourceIntegrationTestData, conn *pgx.Conn) error {
	var query string
	verification := resource.VerificationBuilder(&resource)

	// build query to get the root object
	psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
	sq := psql.Select(fmt.Sprintf("json_agg(%s)", resource.Table.Name)).From(verification.Name)
	// use special filter if it is set.
	if verification.Filter != nil {
		sq = verification.Filter(sq, &resource)
	} else {
		sq = sq.Where(squirrel.Eq{"tags->>'TestId'": resource.Suffix})
	}
	query, args, err := sq.ToSql()
	if err != nil {
		t.Fatal(err)
	}
	row := conn.QueryRow(context.Background(), query, args...)
	data, err := getDataFromRow(row)
	if err != nil {
		t.Fatal(err)
	}

	if err = compareDataWithExpected(verification.ExpectedValues, data); err != nil {
		t.Fatal(fmt.Errorf("verification failed for table %s; %s", resource.Table.Name, err))
	}

	// verify root entry relations
	for _, e := range data {
		id, ok := e["id"]
		if !ok {
			return fmt.Errorf("failed to get parent id for %s", resource.Table.Name)
		}
		if err = verifyRelations(verification.Relations, id, resource.Table.Name, conn); err != nil {
			t.Fatal(fmt.Errorf("verification failed for children of table entry %s; id: %v; %s", resource.Table.Name, id, err))
		}
	}
	return nil
}

// verifyRelations - recursively runs through all the relations and compares their values with expected data
func verifyRelations(relations []*ResourceIntegrationVerification, parentId interface{}, parentName string, conn *pgx.Conn) error {
	for _, relation := range relations {
		// build query to get relation
		psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
		sq := psql.Select(fmt.Sprintf("json_agg(%s)", relation.Name)).
			From(relation.Name).
			LeftJoin(fmt.Sprintf("%[1]s on %[1]s.id = %[3]s.%[2]s", parentName, relation.ForeignKeyName, relation.Name)).
			Where(squirrel.Eq{fmt.Sprintf("%s.id", parentName): parentId})
		query, args, err := sq.ToSql()
		if err != nil {
			return fmt.Errorf("failed to build query for %s", relation.Name)
		}
		row := conn.QueryRow(context.Background(), query, args...)
		data, err := getDataFromRow(row)
		if err != nil {
			return fmt.Errorf("%s -> %s", relation.Name, err)
		}

		if err = compareDataWithExpected(relation.ExpectedValues, data); err != nil {
			return fmt.Errorf("%s -> %s", relation.Name, err)
		}

		// verify relation entry relations
		for _, e := range data {
			id, ok := e["id"]
			if !ok {
				return fmt.Errorf("failed to get parent id for %s", relation.Name)
			}
			err = verifyRelations(relation.Relations, id, relation.Name, conn)
			if err != nil {
				return fmt.Errorf("%s id: %v -> %s", relation.Name, id, err)
			}
		}
	}
	return nil
}

// compareDataWithExpected - runs through expected values and checks if they are satisfied with received data
func compareDataWithExpected(expected []ExpectedValue, received []map[string]interface{}) error {
	var errors []error
	// clone []map that will be compared
	toCompare := make([]map[string]interface{}, len(received))
	for i, row := range received {
		toCompare[i] = make(map[string]interface{})
		for key, value := range row {
			toCompare[i][key] = value
		}
	}

	for _, verification := range expected {
		found := 0
		for i := 0; i < len(toCompare); i++ {
			if toCompare[i] == nil {
				continue // this row is already verified - skip
			}
			err := compareData(verification.Data, toCompare[i])
			if err == nil {
				toCompare[i] = nil // row passed verification - it won't be used
				found++
			} else {
				errors = append(errors, err)
			}

		}
		if verification.Count != found {
			return fmt.Errorf("expected to have %d but got %d entries with one of the %v\nerrors: %v", verification.Count, found, verification.Data, errors)
		}
	}
	return nil
}

// compareData - checks if the second argument has all the entries of the first argument. arguments are jsons - map[string]interface{}
func compareData(verification, row map[string]interface{}) error {
	for k, v := range verification {
		diff := deep.Equal(row[k], v)
		if diff != nil {
			return fmt.Errorf("%+v", diff)
		}
	}
	return nil
}

// simplifyString - prepares the string to be used in resources names
func simplifyString(in string) string {
	// Make a Regex to say we only want letters and numbers
	reg := regexp.MustCompile("[^a-zA-Z0-9]+")
	return strings.ToLower(reg.ReplaceAllString(in, ""))
}

// getDataFromRow - reads the row from db into an array jsons: []map[string]interface{}
func getDataFromRow(row pgx.Row) ([]map[string]interface{}, error) {
	var resp []map[string]interface{}
	var data string

	if err := row.Scan(&data); err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(data), &resp)
	return resp, nil
}

// copyTfFiles - copies tf files that are related to current test
func copyTfFiles(name string) (string, error) {
	workdir := tfDir + name + string(os.PathSeparator)
	if _, err := os.Stat(workdir); os.IsNotExist(err) {
		_ = os.MkdirAll(workdir, os.ModePerm)
	}

	err := cp(tfOrigin+name+".tf", workdir+name+".tf")
	if err != nil {
		return workdir, err
	}

	err = cp(tfOrigin+"variables.tf", workdir+"variables.tf")
	if err != nil {
		return workdir, err
	}

	err = cp(tfOrigin+"provider.tf", workdir+"provider.tf")
	if err != nil {
		return workdir, err
	}

	err = cp(tfOrigin+"versions.tf", workdir+"versions.tf")
	if err != nil {
		return workdir, err
	}

	return workdir, nil
}

// cp - copies file
func cp(src, dst string) error {
	if _, err := os.Stat(src); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}
