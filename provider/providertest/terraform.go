package providertest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
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
	"github.com/stretchr/testify/assert"
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

type ResourceIntegrationVerification struct {
	Name           string
	ForeignKeyName string
	Values         map[string]interface{}
	Filter         func(sq squirrel.SelectBuilder, res *ResourceIntegrationTestData) squirrel.SelectBuilder
	Children       []*ResourceIntegrationVerification
}

func IntegrationTest(t *testing.T, providerCreator func() *provider.Provider, resource ResourceIntegrationTestData) {
	workdir, err := copyTfFiles(resource)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	execPath := os.Getenv("TF_EXEC")
	//execPath := os.Getenv("PATH")
	//execPath, err := tfinstall.Find(ctx)
	//if err != nil {
	//	t.Fatal(err)
	//}
	tf, err := tfexec.NewTerraform(workdir, execPath)
	if err != nil {
		t.Fatal(err)
	}

	name, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	resource.Suffix = simplifyString(name)
	resource.Prefix = simplifyString(resource.Table.Name)
	testSuffix := fmt.Sprintf("test_suffix=%s", resource.Suffix)
	testPrefix := fmt.Sprintf("test_prefix=%s", resource.Prefix)

	log.Println("tf init")
	err = tf.Init(ctx, tfexec.Upgrade(true))
	if err != nil {
		t.Fatal(err)
	}

	log.Println("tf apply")
	err = tf.Apply(ctx, tfexec.Var(testPrefix), tfexec.Var(testSuffix))
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		log.Println("tf destroy")
		err = tf.Destroy(ctx, tfexec.Var(testPrefix), tfexec.Var(testSuffix))
		if err != nil {
			t.Fatal(err)
		}
		err = os.RemoveAll(workdir)
		if err != nil {
			t.Fatal(err)
		}
		log.Println("done")
	}()

	log.Println("setup database")
	conn, err := setupDatabase()
	assert.Nil(t, err)

	log.Println("fetch resources")
	testProvider := providerCreator()

	testProvider.Logger = logging.New(hclog.DefaultOptions)

	//cfg, err := testProvider.GetProviderConfig(context.Background(), &cqproto.GetProviderConfigRequest{})
	//if err != nil {
	//	t.Fatal(err)
	//}
	//data := cfg.Config

	f := hclwrite.NewFile()
	f.Body().AppendBlock(gohcl.EncodeAsBlock(resource.Config, "configuration"))
	data, err := convert.Bytes(f.Bytes(), "config.json", convert.Options{})
	hack := map[string]interface{}{}
	_ = json.Unmarshal(data, &hack)

	data, _ = json.Marshal(hack["configuration"].([]interface{})[0])
	assert.Nil(t, err)

	testProvider.Configure = resource.Configure
	_, err = testProvider.ConfigureProvider(context.Background(), &cqproto.ConfigureProviderRequest{
		CloudQueryVersion: "",
		Connection:        cqproto.ConnectionDetails{DSN: "host=localhost user=postgres password=pass DB.name=postgres port=5432"},
		Config:            data,
	})
	assert.Nil(t, err)

	_ = testProvider.FetchResources(context.Background(), &cqproto.FetchResourcesRequest{Resources: []string{findResourceFromTableName(resource.Table, testProvider.ResourceMap)}}, fakeResourceSender{})
	assert.Nil(t, err)

	log.Println("verify fields")
	err = verifyFields(t, resource, conn)
	assert.Nil(t, err)
}

func verifyFields(t *testing.T, resource ResourceIntegrationTestData, conn *pgx.Conn) error {
	//get first root object
	var query string
	psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
	sq := psql.Select(fmt.Sprintf("json_agg(%s)", resource.Table.Name)).From(resource.Table.Name)

	verification := resource.VerificationBuilder(&resource)
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
	log.Println(data)
	if len(data) != 1 {
		t.Fatalf("expected to have  1 entry at table %s got %d", resource.Table.Name, len(data))
	}

	if err = compareData(verification.Values, data[0]); err != nil {
		t.Fatal(fmt.Errorf("verification failed for table %s; err: %s", resource.Table.Name, err))
	}
	if err = verifyChildren(verification.Children, data[0], resource.Table.Name, conn); err != nil {
		t.Fatal(fmt.Errorf("verification failed for children of table %s; err: %s", resource.Table.Name, err))
	}
	return nil
}

func verifyChildren(children []*ResourceIntegrationVerification, parrent map[string]interface{}, parrentName string, conn *pgx.Conn) error {
	for _, child := range children {
		psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
		id, ok := parrent["id"].(string)
		if !ok {
			return fmt.Errorf("failed to get parrent id for %s", child.Name)
		}
		query, args, err := psql.Select(fmt.Sprintf("json_agg(%s)", child.Name)).
			From(child.Name).
			LeftJoin(fmt.Sprintf("%[1]s on %[1]s.id = %[3]s.%[2]s", parrentName, child.ForeignKeyName, child.Name)).
			Where(squirrel.Eq{fmt.Sprintf("%s.id", parrentName): id}).
			ToSql()
		if err != nil {
			return fmt.Errorf("failed to build child sql for %s", child.Name)
		}
		row := conn.QueryRow(context.Background(), query, args...)
		data, err := getDataFromRow(row)
		if err != nil {
			return err
		}
		if err = compareData(child.Values, data[0]); err != nil {
			return err
		}
		err = verifyChildren(child.Children, data[0], child.Name, conn)
		if err != nil {
			return fmt.Errorf("%s -> %s", child.Name, err)
		}
	}
	return nil
}

func compareData(verification, row map[string]interface{}) error {
	for k, v := range verification {
		diff := deep.Equal(row[k], v)
		if diff != nil {
			return fmt.Errorf("data does not match expected %s to be %v but got %v; diff: %+v", k, v, row[k], diff)
		}
	}
	return nil
}

func simplifyString(in string) string {
	// Make a Regex to say we only want letters and numbers
	reg := regexp.MustCompile("[^a-zA-Z0-9]+")
	return strings.ToLower(reg.ReplaceAllString(in, ""))
}

func getDataFromRow(row pgx.Row) ([]map[string]interface{}, error) {
	var resp []map[string]interface{}
	var data string

	if err := row.Scan(&data); err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(data), &resp)
	return resp, nil
}

//copies necessary for current test files
func copyTfFiles(resource ResourceIntegrationTestData) (string, error) {
	workdir := tfDir + resource.Table.Name + "/"
	if _, err := os.Stat(workdir); os.IsNotExist(err) {
		_ = os.MkdirAll(workdir, os.ModePerm)
	}

	err := copy(tfOrigin+resource.Table.Name+".tf", workdir+resource.Table.Name+".tf")
	if err != nil {
		return workdir, err
	}

	err = copy(tfOrigin+"variables.tf", workdir+"variables.tf")
	if err != nil {
		return workdir, err
	}

	err = copy(tfOrigin+"provider.tf", workdir+"provider.tf")
	if err != nil {
		return workdir, err
	}

	err = copy(tfOrigin+"versions.tf", workdir+"versions.tf")
	if err != nil {
		return workdir, err
	}

	return workdir, nil
}

func copy(src, dst string) error {
	if _, err := os.Stat(src); err != nil {
		//fmt.Printf("File does not exist\n");
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
