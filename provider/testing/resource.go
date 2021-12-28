package testing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/cloudquery/cq-provider-sdk/provider"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/cloudquery/cq-provider-sdk/testlog"
	"github.com/cloudquery/faker/v3"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/google/go-cmp/cmp"
	"github.com/hashicorp/go-hclog"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
)

type ResourceTestCase struct {
	Provider       *provider.Provider
	Table          *schema.Table
	Config         string
	SnapshotsDir   string
	SkipEmptyJsonB bool
}

var ignoreColumns = map[string]bool{
	"last_updated": true,
	"cq_id":        true,
	"meta":         true,
}

// IntegrationTest - creates resources using terraform, fetches them to db and compares with expected values
func TestResource(t *testing.T, resource ResourceTestCase) {
	t.Parallel()
	t.Helper()
	if err := faker.SetRandomMapAndSliceMinSize(1); err != nil {
		t.Fatal(err)
	}
	if err := faker.SetRandomMapAndSliceMaxSize(1); err != nil {
		t.Fatal(err)
	}

	// No need for configuration or db connection, get it out of the way first
	// testTableIdentifiersForProvider(t, resource.Provider)

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

	l := testlog.New(t)
	l.SetLevel(hclog.Debug)
	resource.Provider.Logger = l
	tableCreator := provider.NewTableCreator(l)
	if err := tableCreator.CreateTable(context.Background(), conn, resource.Table, nil); err != nil {
		assert.FailNow(t, fmt.Sprintf("failed to create tables %s", resource.Table.Name), err)
	}

	if err := deleteTables(conn, resource.Table); err != nil {
		t.Fatal(err)
	}

	if resource.SnapshotsDir != "" {
		if err = fetch(t, &resource); err != nil {
			t.Fatal(err)
		}

		// run this if snapshot testing is enabled
		equal, err := verifyTable(t, conn, resource.Table, resource)
		if err != nil {
			t.Fatal(err)
		}
		if !equal {
			t.Error("results not equal")
		}
	} else {
		// run this if snapshot testing is disabled. i.e mock tests. This will probabbly go away
		verifyNoEmptyColumns(t, resource, conn)
	}

	// verifyNoEmptyColumns(t, resource, conn)

	if err := conn.Conn().Close(ctx); err != nil {
		t.Fatal(err)
	}

}

// fetch - fetches resources from the cloud and puts them into database. database config can be specified via DATABASE_URL env variable
func fetch(t *testing.T, resource *ResourceTestCase) error {
	t.Logf("%s fetch resources", resource.Table.Name)

	if _, err := resource.Provider.ConfigureProvider(context.Background(), &cqproto.ConfigureProviderRequest{
		CloudQueryVersion: "",
		Connection: cqproto.ConnectionDetails{DSN: getEnv("DATABASE_URL",
			"host=localhost user=postgres password=pass DB.name=postgres port=5432")},
		Config:        []byte(resource.Config),
		DisableDelete: true,
	}); err != nil {
		return err
	}

	var resourceSender = &fakeResourceSender{
		Errors: []string{},
	}

	if err := resource.Provider.FetchResources(context.Background(),
		&cqproto.FetchResourcesRequest{
			Resources: []string{findResourceFromTableName(resource.Table, resource.Provider.ResourceMap)},
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

func verifyTable(t *testing.T, conn *pgxpool.Conn, table *schema.Table, resource ResourceTestCase) (bool, error) {
	t.Helper()
	res := true
	columnsToIgnore := []string{
		"cq_id",
		"meta",
	}
	// this order of insertion is not the same so we try to order by all columns which are constant
	// Future note - if api will return results not in the same order we will have to do a smarter diff that doesn't rely on order
	// this is not hard but just will provider worse debug info on what is changed
	columns := ""
	for _, c := range table.Columns {
		if !ignoreColumns[c.Name] && !strings.HasSuffix(c.Name, "_cq_id") && !c.IgnoreInIntTests {
			columns += "\"" + c.Name + "\"" + ","
		} else {
			columnsToIgnore = append(columnsToIgnore, c.Name)
		}
	}

	columns = strings.TrimRight(columns, ",")
	s := sq.StatementBuilder.
		PlaceholderFormat(sq.Dollar).
		Select(fmt.Sprintf("json_agg(%s order by %s)", table.Name, columns)).
		From(table.Name)

	query, args, err := s.ToSql()
	if err != nil {
		return false, err
	}

	var data []map[string]interface{}
	if err := pgxscan.Get(context.Background(), conn, &data, query, args...); err != nil {
		return false, err
	}
	removeColumns(data, columnsToIgnore)

	b, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return false, err
	}

	snapshotPath := path.Join(resource.SnapshotsDir, table.Name+".snapshot")
	if err := os.MkdirAll(resource.SnapshotsDir, os.ModePerm); err != nil {
		return false, err
	}

	// nolint
	if _, err := os.Stat(snapshotPath); err == nil {
		// snapshot already exist check if content is equal, if not fail
		snapshotContent, err := os.ReadFile(snapshotPath)
		if err != nil {
			return false, err
		}
		var savedData []map[string]interface{}
		if err := json.Unmarshal(snapshotContent, &savedData); err != nil {
			return false, err
		}

		diff := cmp.Diff(data, savedData)
		if diff != "" {
			t.Log("found diff")
			t.Log(diff)
			t.Logf("Saving snapshot to %s.tmp\n", snapshotPath)
			// open output file
			if err := os.WriteFile(snapshotPath+".tmp", b, 0644); err != nil {
				return false, err
			}
			res = false
		}
	} else if errors.Is(err, os.ErrNotExist) {
		t.Logf("Previous snapshot doesn't exist. Saving snapshot to %s\n", snapshotPath)
		// open output file
		if err := os.WriteFile(snapshotPath, b, 0644); err != nil {
			return false, err
		}
		res = false
	} else {
		return false, err
	}

	for _, relation := range table.Relations {
		equal, err := verifyTable(t, conn, relation, resource)
		if err != nil {
			return false, err
		}
		if !equal {
			res = false
		}
	}
	return res, nil
}

func removeColumns(res []map[string]interface{}, ignoreColumns []string) {
	ignoreColumnsMap := make(map[string]bool, len(ignoreColumns))
	for _, c := range ignoreColumns {
		ignoreColumnsMap[c] = true
	}

	for i := range res {
		for c := range res[i] {
			if ignoreColumnsMap[c] {
				res[i][c] = "[unstable_column]"
			}
		}
	}
}

func deleteTables(conn *pgxpool.Conn, table *schema.Table) error {
	s := sq.Delete(table.Name)
	sql, args, err := s.ToSql()
	if err != nil {
		return err
	}

	_, err = conn.Exec(context.TODO(), sql, args...)
	if err != nil {
		return err
	}
	return nil
}

func verifyNoEmptyColumns(t *testing.T, tc ResourceTestCase, conn pgxscan.Querier) {
	// Test that we don't have missing columns and have exactly one entry for each table
	for _, table := range getTablesFromMainTable(tc.Table) {
		query := fmt.Sprintf("select * FROM %s ", table)
		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			count += 1
		}
		if count < 1 {
			t.Fatalf("expected to have at least 1 entry at table %s got %d", table, count)
		}
		if tc.SkipEmptyJsonB {
			continue
		}
		query = fmt.Sprintf("select t.* FROM %s as t WHERE to_jsonb(t) = jsonb_strip_nulls(to_jsonb(t))", table)
		rows, err = conn.Query(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		count = 0
		for rows.Next() {
			count += 1
		}
		if count < 1 {
			t.Fatalf("row at table %s has an empty column", table)
		}
	}
}

func findResourceFromTableName(table *schema.Table, tables map[string]*schema.Table) string {
	for resource, t := range tables {
		if table.Name == t.Name {
			return resource
		}
	}
	return ""
}

type fakeResourceSender struct {
	Errors []string
}

func (f *fakeResourceSender) Send(r *cqproto.FetchResourcesResponse) error {
	if r.Error != "" {
		fmt.Printf(r.Error)
		f.Errors = append(f.Errors, r.Error)
	}
	return nil
}

var (
	dbConnOnce sync.Once
	pool       *pgxpool.Pool
	dbErr      error
)

func setupDatabase() (*pgxpool.Pool, error) {
	dbConnOnce.Do(func() {
		var dbCfg *pgxpool.Config
		dbCfg, dbErr = pgxpool.ParseConfig(getEnv("DATABASE_URL", "host=localhost user=postgres password=pass DB.name=postgres port=5432"))
		if dbErr != nil {
			return
		}
		ctx := context.Background()
		dbCfg.MaxConns = 15
		dbCfg.LazyConnect = true
		pool, dbErr = pgxpool.ConnectConfig(ctx, dbCfg)
	})
	return pool, dbErr

}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getTablesFromMainTable(table *schema.Table) []string {
	var res []string
	res = append(res, table.Name)
	for _, t := range table.Relations {
		res = append(res, getTablesFromMainTable(t)...)
	}
	return res
}

func testTableIdentifiersForProvider(t *testing.T, prov *provider.Provider) {
	t.Run("testTableIdentifiersForProvider", func(t *testing.T) {
		t.Parallel()
		for _, res := range prov.ResourceMap {
			res := res
			t.Run(res.Name, func(t *testing.T) {
				testTableIdentifiers(t, res)
			})
		}
	})
}

func testTableIdentifiers(t *testing.T, table *schema.Table) {
	t.Parallel()
	assert.NoError(t, schema.ValidateTable(table))

	for _, res := range table.Relations {
		res := res
		t.Run(res.Name, func(t *testing.T) {
			testTableIdentifiers(t, res)
		})
	}
}
