package testing

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Masterminds/squirrel"
	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/cloudquery/cq-provider-sdk/logging"
	"github.com/cloudquery/cq-provider-sdk/provider"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/go-test/deep"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/hashicorp/terraform-exec/tfexec"
	"github.com/tmccombs/hcl2json/convert"
)

var (
	// Make a Regex to say we only want letters and numbers
	tfVarRegex = regexp.MustCompile("[^a-zA-Z0-9]+")
)

type ResourceIntegrationTestData struct {
	Table               *schema.Table
	Config              interface{}
	Resources           []string
	Configure           func(logger hclog.Logger, data interface{}) (schema.ClientMeta, error)
	Suffix              string
	Prefix              string
	VerificationBuilder func(res *ResourceIntegrationTestData) ResourceIntegrationVerification
	Workdir             string
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

	// prepare terraform variables
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}

	// whether want TF to apply and create resources instead of running a fetch on existing resources
	var tfApplyResources = getEnv("TF_APPLY_RESOURCES", "") == "1"
	var varPrefix = simplifyString(resource.Table.Name)
	var varSuffix = simplifyString(hostname)

	prefix := getEnv("TF_VAR_PREFIX", "")
	if prefix != "" {
		varPrefix = prefix
	} else if !tfApplyResources {
		t.Fatalf("Missing resource TF_VAR_PREFIX either set this environment variable or use TF_APPLY_RESOURCES=1")
	}

	suffix := getEnv("TF_VAR_SUFFIX", "")
	if suffix != "" {
		varSuffix = suffix
	} else if !tfApplyResources {
		t.Fatalf("Missing resource TF_VAR_SUFFIX either set this environment or use TF_APPLY_RESOURCES=1")
	}

	// finally set picked prefix/suffix to resource
	resource.Prefix = varPrefix
	resource.Suffix = varSuffix

	if tfApplyResources {
		tf, err := setup(&resource)
		if err != nil {
			t.Fatal(err)
		}

		teardown, err := deploy(tf, &resource)
		if teardown != nil && getEnv("TF_NO_DESTROY", "") != "1" {
			defer func() {
				if err = teardown(); err != nil {
					t.Fatal(err)
				}
			}()
		} else {
			defer func() {
				log.Printf("%s RESOURCES WERE NOT DESTROYTED. destroy them manually.\n", resource.Table.Name)
			}()
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	log.Printf("%s verify fields\n", resource.Table.Name)
	pool, err := setupDatabase()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()

	l := logging.New(hclog.DefaultOptions)
	tableCreator := provider.NewTableCreator(l)
	if err := tableCreator.CreateTable(context.Background(), conn, resource.Table, nil); err != nil {
		assert.FailNow(t, fmt.Sprintf("failed to create tables %s", resource.Table.Name), err)
	}

	if err = fetch(providerCreator, &resource); err != nil {
		t.Fatal(err)
	}

	if err = verifyFields(resource, conn); err != nil {
		t.Fatal(err)
	}

	if err := conn.Conn().Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// setup - puts *.tf files into isolated test dir and creates the instance of terraform
func setup(resource *ResourceIntegrationTestData) (*tfexec.Terraform, error) {
	var err error
	lookPath := getEnv("TF_EXEC_PATH", "")
	if lookPath == "" {
		lookPath = "terraform"
	}
	execPath, err := exec.LookPath(lookPath)
	if err != nil {
		return nil, err
	}
	tf, err := tfexec.NewTerraform(resource.Workdir, execPath)
	if err != nil {
		return nil, err
	}
	if err = enableTerraformLog(tf, resource.Workdir); err != nil {
		return nil, err
	}
	return tf, nil
}

// deploy - uses terraform to deploy resources and builds teardown function. deployment timeout can be set via TF_EXEC_TIMEOUT env variable
func deploy(tf *tfexec.Terraform, resource *ResourceIntegrationTestData) (func() error, error) {
	tfVars := []*tfexec.VarOption{
		tfexec.Var("test_suffix=" + resource.Suffix),
		tfexec.Var("test_prefix=" + resource.Prefix),
	}
	tfDestoryOptions := make([]tfexec.DestroyOption, 0, len(resource.Resources)+2)
	tfApplyOptions := make([]tfexec.ApplyOption, 0, len(resource.Resources)+2)
	tfDestoryOptions = append(tfDestoryOptions, tfVars[0], tfVars[1])
	tfApplyOptions = append(tfApplyOptions, tfVars[0], tfVars[1])

	for _, f := range resource.Resources {
		tfDestoryOptions = append(tfDestoryOptions, tfexec.Target("target="+f))
		tfApplyOptions = append(tfApplyOptions, tfexec.Target("target="+f))
	}

	teardown := func() error {
		if len(resource.Resources) == 0 {
			// we have nothing to destroy so we can just skip
			return nil
		}
		log.Printf("%s destroy\n", resource.Table.Name)
		err := tf.Destroy(context.Background(), tfDestoryOptions...)
		if err != nil {
			return err
		}
		log.Printf("%s done\n", resource.Table.Name)
		return nil
	}

	if len(resource.Resources) == 0 {
		// we have nothing to deploy so we can just skip
		return teardown, nil
	}

	ctx := context.Background()
	if i, err := strconv.Atoi(getEnv("TF_EXEC_TIMEOUT", "")); err == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(i)*time.Minute)
		defer cancel()
	}

	log.Printf("%s tf init\n", resource.Table.Name)
	if err := tf.Init(ctx, tfexec.Upgrade(true)); err != nil {
		return teardown, err
	}

	ticker := time.NewTicker(5 * time.Minute)
	startTime := time.Now()
	applyDone := make(chan bool)
	go func() {
		for {
			select {
			case <-ctx.Done():
			case <-applyDone:
				return
			case timestamp := <-ticker.C:
				log.Printf("%s applying for %.2f minutes", resource.Table.Name, timestamp.Sub(startTime).Minutes())
			}
		}
	}()

	log.Printf("%s tf apply -var test_prefix=%s -var test_suffix=%s\n", resource.Table.Name, resource.Prefix, resource.Suffix)
	err := tf.Apply(ctx, tfApplyOptions...)
	applyDone <- true
	if err != nil {
		return teardown, err
	}

	return teardown, nil
}

// fetch - fetches resources from the cloud and puts them into database. database config can be specified via DATABASE_URL env variable
func fetch(providerCreator func() *provider.Provider, resource *ResourceIntegrationTestData) error {
	log.Printf("%s fetch resources\n", resource.Table.Name)
	testProvider := providerCreator()
	testProvider.Logger = logging.New(hclog.DefaultOptions)

	// generate a config for provider
	f := hclwrite.NewFile()
	f.Body().AppendBlock(gohcl.EncodeAsBlock(resource.Config, "configuration"))
	data, err := convert.Bytes(f.Bytes(), "config.json", convert.Options{})
	if err != nil {
		return err
	}
	c := map[string]interface{}{}
	_ = json.Unmarshal(data, &c)
	data, err = json.Marshal(c["configuration"].([]interface{})[0])
	if err != nil {
		return err
	}

	testProvider.Configure = resource.Configure
	if _, err = testProvider.ConfigureProvider(context.Background(), &cqproto.ConfigureProviderRequest{
		CloudQueryVersion: "",
		Connection: cqproto.ConnectionDetails{DSN: getEnv("DATABASE_URL",
			"host=localhost user=postgres password=pass DB.name=postgres port=5432")},
		Config:        data,
		DisableDelete: true,
	}); err != nil {
		return err
	}

	var resourceSender = &fakeResourceSender{
		Errors: []string{},
	}

	if err = testProvider.FetchResources(context.Background(),
		&cqproto.FetchResourcesRequest{
			Resources: []string{findResourceFromTableName(resource.Table, testProvider.ResourceMap)},
		},
		resourceSender,
	); err != nil {
		return err
	}

	if len(resourceSender.Errors) > 0 {
		return fmt.Errorf("error/s occur during test, %s", strings.Join(resourceSender.Errors, ", "))
	}

	return nil
}

// enableTerraformLog - sets the path for terraform log files for current test
func enableTerraformLog(tf *tfexec.Terraform, workdir string) error {
	abs, err := filepath.Abs(workdir)
	if err != nil {
		return err
	}
	dst := filepath.Join(abs, string(os.PathSeparator), "tflog")
	if _, err = os.Create(dst); err != nil {
		return err
	}
	if err = tf.SetLogPath(dst); err != nil {
		return err
	}

	tf.SetLogger(log.Default())
	return nil
}

// verifyFields - gets the root db entry and check all its expected relations
func verifyFields(resource ResourceIntegrationTestData, conn pgxscan.Querier) error {
	verification := resource.VerificationBuilder(&resource)

	// build query to get the root object
	sq := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Select(fmt.Sprintf("json_agg(%s)", verification.Name)).
		From(verification.Name)
	// use special filter if it is set.
	if verification.Filter != nil {
		sq = verification.Filter(sq, &resource)
	} else {
		sq = sq.Where(squirrel.Eq{"tags->>'TestId'": resource.Suffix})
	}
	query, args, err := sq.ToSql()
	if err != nil {
		return fmt.Errorf("%s -> %w", verification.Name, err)
	}

	var data []map[string]interface{}
	if err := pgxscan.Get(context.Background(), conn, &data, query, args...); err != nil {
		return fmt.Errorf("%s -> %w", verification.Name, err)
	}

	if err = compareDataWithExpected(verification.ExpectedValues, data); err != nil {
		return fmt.Errorf("verification failed for table %s; %w", resource.Table.Name, err)
	}

	// verify root entry relations
	for _, e := range data {
		id, ok := e["cq_id"]
		if !ok {
			return fmt.Errorf("failed to get parent id for %s", resource.Table.Name)
		}
		if err = verifyRelations(verification.Relations, id, resource.Table.Name, conn); err != nil {
			return fmt.Errorf("verification failed for relations of table entry %s; cq_id: %v -> %w", resource.Table.Name, id, err)
		}
	}
	return nil
}

// verifyRelations - recursively runs through all the relations and compares their values with expected data
func verifyRelations(relations []*ResourceIntegrationVerification, parentId interface{}, parentName string, conn pgxscan.Querier) error {
	for _, relation := range relations {
		// build query to get relation
		sq := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).
			Select(fmt.Sprintf("json_agg(%s)", relation.Name)).
			From(relation.Name).
			LeftJoin(fmt.Sprintf("%[1]s on %[1]s.cq_id = %[3]s.%[2]s", parentName, relation.ForeignKeyName, relation.Name)).
			Where(squirrel.Eq{fmt.Sprintf("%s.cq_id", parentName): parentId})
		query, args, err := sq.ToSql()

		if err != nil {
			return fmt.Errorf("%s -> %w", relation.Name, err)
		}

		var data []map[string]interface{}
		if err := pgxscan.Get(context.Background(), conn, &data, query, args...); err != nil {
			return fmt.Errorf("%s -> %w", relation.Name, err)
		}

		if err = compareDataWithExpected(relation.ExpectedValues, data); err != nil {
			return fmt.Errorf("%s -> %w", relation.Name, err)
		}

		// verify relation entry relations
		for _, e := range data {
			id, ok := e["cq_id"]
			if !ok {
				return fmt.Errorf("failed to get parent id for %s", relation.Name)
			}
			if err = verifyRelations(relation.Relations, id, relation.Name, conn); err != nil {
				return fmt.Errorf("%s cq_id: %v -> %w", relation.Name, id, err)
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
			if err != nil {
				errors = append(errors, err)
				continue
			}
			toCompare[i] = nil // row passed verification - it won't be used
			found++
		}
		// verification.Count == 0 means we expect at least 1 result
		if verification.Count == 0 && found > 0 {
			continue
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
	return strings.ToLower(tfVarRegex.ReplaceAllString(in, ""))
}
