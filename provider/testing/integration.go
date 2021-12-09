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
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/hashicorp/terraform-exec/tfexec"
	"github.com/r3labs/diff/v2"
	"github.com/tmccombs/hcl2json/convert"
)

const (
	tfDir         = "./.test/"
	infraFilesDir = "./infra/"
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
	VerificationBuilder VerificationBuilder
	workdir             string
}

type VerificationBuilder func(res *ResourceIntegrationTestData) []Verification

// Verification - a set of verification rules to compare with fetched data
type Verification struct {
	Filter         squirrel.And
	Name           string
	ExpectedValues map[string]interface{}
	Relations      []Verification
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
	if resource.Resources != nil {
		resource.workdir, err = copyTfFiles(resource.Table.Name, resource.Resources...)
	} else {
		resource.workdir, err = copyTfFiles(resource.Table.Name, fmt.Sprintf("%s.tf", resource.Table.Name))
	}

	if err != nil {
		// remove workdir
		if e := os.RemoveAll(resource.workdir); e != nil {
			return nil, fmt.Errorf("failed to RemoveAll after: %w\n reason:%s", err, e)
		}
		return nil, err
	}

	lookPath := getEnv("TF_EXEC_PATH", "")
	if lookPath == "" {
		lookPath = "terraform"
	}
	execPath, err := exec.LookPath(lookPath)
	if err != nil {
		return nil, err
	}
	tf, err := tfexec.NewTerraform(resource.workdir, execPath)
	if err != nil {
		return nil, err
	}
	if err = enableTerraformLog(tf, resource.workdir); err != nil {
		return nil, err
	}
	return tf, nil
}

// deploy - uses terraform to deploy resources and builds teardown function. deployment timeout can be set via TF_EXEC_TIMEOUT env variable
func deploy(tf *tfexec.Terraform, resource *ResourceIntegrationTestData) (func() error, error) {
	testSuffix := fmt.Sprintf("test_suffix=%s", resource.Suffix)
	testPrefix := fmt.Sprintf("test_prefix=%s", resource.Prefix)

	teardown := func() error {
		log.Printf("%s destroy\n", resource.Table.Name)
		err := tf.Destroy(context.Background(), tfexec.Var(testPrefix),
			tfexec.Var(testSuffix))
		if err != nil {
			return err
		}
		log.Printf("%s cleanup\n", resource.Table.Name)
		if err := os.RemoveAll(resource.workdir); err != nil {
			return err
		}
		log.Printf("%s done\n", resource.Table.Name)
		return nil
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
	err := tf.Apply(ctx, tfexec.Var(testPrefix), tfexec.Var(testSuffix))
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
	return nil
}

// verifyFields - gets the root db entry and check all its expected relations
func verifyFields(resource ResourceIntegrationTestData, conn pgxscan.Querier) error {
	verification := resource.VerificationBuilder(&resource)
	foreignKeys := getForeignKeys(resource.Table)

	violations, err := verifyRelations(context.Background(), nil, foreignKeys, verification, conn)
	if err != nil {
		return fmt.Errorf("%s -> %w", resource.Table.Name, err)
	}

	if len(violations) > 0 {
		return fmt.Errorf("failed to validate data.\n%s", strings.Join(violations, "\n"))
	}
	return nil
}

func getForeignKeys(table *schema.Table) map[string]string {
	result := make(map[string]string, len(table.Relations))
	for _, c := range table.Columns {
		if strings.Contains(c.Name, "_cq_id") {
			result[table.Name] = c.Name
		}
	}

	for _, relation := range table.Relations {
		data := getForeignKeys(relation)
		for k, v := range data {
			result[k] = v
		}
	}

	return result
}

// verifyRelations - recursively runs through all the relations
func verifyRelations(ctx context.Context, parentId interface{}, foreignKeys map[string]string, relations []Verification, conn pgxscan.Querier) ([]string, error) {
	var violations []string
	for _, relation := range relations {
		sq := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).
			Select(fmt.Sprintf("json_agg(%s)", relation.Name)).
			From(relation.Name)

		filters := squirrel.And{}
		if parentId != nil {
			foreignKey, ok := foreignKeys[relation.Name]
			if !ok {
				return nil, fmt.Errorf("failed to find foreignKey for %s", relation.Name)
			}
			filters = append(filters, squirrel.Eq{foreignKey: parentId})
		}
		if relation.Filter != nil {
			filters = append(filters, relation.Filter)
		}
		// add verification fields as query parameters but ignore complex types like arrays and maps
		for k, v := range relation.ExpectedValues {
			_, ismap := v.(map[string]interface{})
			_, isslice := v.([]interface{})
			// ignore complex types in query
			if ismap || isslice {
				continue
			}

			filters = append(filters, squirrel.Eq{k: v})
		}
		sq = sq.Where(filters)
		query, args, err := sq.ToSql()
		if err != nil {
			return nil, fmt.Errorf("%s -> %w", relation.Name, err)
		}

		queryToExecute := fmt.Sprintf("query: %s, args: %v", query, args)
		var data []map[string]interface{}
		if err := pgxscan.Get(ctx, conn, &data, query, args...); err != nil {
			return nil, fmt.Errorf("%s -> %s:%w", relation.Name, queryToExecute, err)
		}

		if len(data) > 0 {
			// verify relation entry relations
			diffs, smallestDiffId, smallestDiffLenght, err := resolveDiffs(relation, data)
			if err != nil {
				return nil, fmt.Errorf("failed to diff values of %s -> %w", relation.Name, err)
			}

			if smallestDiffLenght == 0 {
				v, err := verifyRelations(ctx, smallestDiffId, foreignKeys, relation.Relations, conn)
				if err != nil {
					return nil, fmt.Errorf("%s cq_id: %v -> %w", relation.Name, smallestDiffId, err)
				}
				violations = append(violations, v...)
			} else {
				violations = append(violations, fmt.Sprintf("fields does not match: path | expected != retrieved: %s", strings.Join(diffs[smallestDiffId], ",")))
			}

		} else {
			violationString := fmt.Sprintf("Query: `%s` returned no results", queryToExecute)
			violations = append(violations, violationString)
		}
	}
	return violations, nil
}

// resolveDiffs - finds which object from retrieved matches expected object the most
func resolveDiffs(relation Verification, retrieved []map[string]interface{}) (map[interface{}][]string, interface{}, int, error) {
	smallestDiffCount := -1
	var smallestDiffId interface{}
	diffs := make(map[interface{}][]string, len(retrieved))
	for _, r := range retrieved {
		id, ok := r["cq_id"]
		if !ok {
			return diffs, smallestDiffId, smallestDiffCount, fmt.Errorf("object does not have cq_id field")
		}
		// check if current retrieved object matches expected values including complex fields
		d, e := diff.Diff(relation.ExpectedValues, r, diff.AllowTypeMismatch(true))
		if e != nil {
			return diffs, smallestDiffId, smallestDiffCount, e
		}
		var diffResult []string
		// skipping all differences that exist in retrieved object and does not exist in expected object
		for _, c := range d {
			if c.Type == diff.UPDATE || c.Type == diff.DELETE {
				diffResult = append(diffResult, fmt.Sprintf("%s | %v(%T) != %v(%T)", strings.Join(c.Path, "."), c.From, c.From, c.To, c.To))
			}
		}
		diffs[id] = diffResult
	}
	for k, v := range diffs {
		diffCount := len(v)
		if smallestDiffCount == -1 {
			smallestDiffCount = diffCount
			smallestDiffId = k
		} else if diffCount < smallestDiffCount {
			smallestDiffCount = diffCount
			smallestDiffId = k
		}
	}
	return diffs, smallestDiffId, smallestDiffCount, nil
}

// simplifyString - prepares the string to be used in resources names
func simplifyString(in string) string {
	return strings.ToLower(tfVarRegex.ReplaceAllString(in, ""))
}

// copyTfFiles - copies tf files that are related to current test
func copyTfFiles(testName string, tfTestFiles ...string) (string, error) {
	workdir := filepath.Join(tfDir, testName)
	if _, err := os.Stat(workdir); os.IsNotExist(err) {
		if err := os.MkdirAll(workdir, os.ModePerm); err != nil {
			return workdir, err
		}
	} else if err != nil {
		return "", err
	}

	files := make(map[string]string)
	for _, tftf := range tfTestFiles {
		files[filepath.Join(infraFilesDir, tftf)] = filepath.Join(workdir, tftf)
	}
	files[filepath.Join(infraFilesDir, "terraform.tf")] = filepath.Join(workdir, "terraform.tf")
	files[filepath.Join(infraFilesDir, "provider.tf")] = filepath.Join(workdir, "provider.tf")
	files[filepath.Join(infraFilesDir, "variables.tf")] = filepath.Join(workdir, "variables.tf")

	for src, dst := range files {
		if _, err := os.Stat(src); err != nil {
			return "", err
		}

		in, err := os.ReadFile(src)
		if err != nil {
			return "", err
		}
		if err := os.WriteFile(dst, in, 0644); err != nil {
			return "", err
		}
	}
	return workdir, nil
}
