package providertest

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/cloudquery/faker/v3"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/tmccombs/hcl2json/convert"
	"os"
	"testing"

	"github.com/georgysavva/scany/pgxscan"

	"github.com/cloudquery/cq-provider-sdk/logging"
	"github.com/cloudquery/cq-provider-sdk/provider"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/hashicorp/go-hclog"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
)

type ResourceTestData struct {
	Table     *schema.Table
	Config    interface{}
	Resources []string
	Configure func(logger hclog.Logger, data interface{}) (schema.ClientMeta, error)
}

func TestResource(t *testing.T, providerCreator func() *provider.Provider, resource ResourceTestData) {
	if err := faker.SetRandomMapAndSliceMinSize(1); err != nil {
		t.Fatal(err)
	}
	if err := faker.SetRandomMapAndSliceMaxSize(1); err != nil {
		t.Fatal(err)
	}
	conn, err := setupDatabase()
	if err != nil {
		t.Fatal(err)
	}
	// Write configuration as a block and extract it out passing that specific block data as part of the configure provider
	f := hclwrite.NewFile()
	f.Body().AppendBlock(gohcl.EncodeAsBlock(resource.Config, "configuration"))
	data, err := convert.Bytes(f.Bytes(), "config.json", convert.Options{})
	hack := map[string]interface{}{}
	json.Unmarshal(data, &hack)
	data, _ = json.Marshal(hack["configuration"].([]interface{})[0])
	assert.Nil(t, err)

	testProvider := providerCreator()
	testProvider.Logger = logging.New(hclog.DefaultOptions)
	testProvider.Configure = resource.Configure
	_, err = testProvider.ConfigureProvider(context.Background(), &cqproto.ConfigureProviderRequest{
		CloudQueryVersion: "",
		Connection:        cqproto.ConnectionDetails{DSN: "host=localhost user=postgres password=pass DB.name=postgres port=5432"},
		Config:            data,
	})
	assert.Nil(t, err)

	_ = testProvider.FetchResources(context.Background(), &cqproto.FetchResourcesRequest{Resources: []string{findResourceFromTableName(resource.Table, testProvider.ResourceMap)}}, fakeResourceSender{})
	assert.Nil(t, err)
	verifyNoEmptyColumns(t, resource, conn)
}

func findResourceFromTableName(table *schema.Table, tables map[string]*schema.Table) string {
	for resource, t := range tables {
		if table.Name == t.Name {
			return resource
		}
	}
	return ""
}

type fakeResourceSender struct{}

func (f fakeResourceSender) Send(_ *cqproto.FetchResourcesResponse) error {
	return nil
}

func setupDatabase() (*pgx.Conn, error) {
	dbCfg, err := pgx.ParseConfig(getEnv("DATABASE_URL",
		"host=localhost user=postgres password=pass DB.name=postgres port=5432"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse config. %w", err)
	}
	ctx := context.Background()
	conn, err := pgx.ConnectConfig(ctx, dbCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database. %w", err)
	}
	return conn, nil

}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func verifyNoEmptyColumns(t *testing.T, tc ResourceTestData, conn pgxscan.Querier) {
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

func getTablesFromMainTable(table *schema.Table) []string {
	var res []string
	res = append(res, table.Name)
	for _, t := range table.Relations {
		res = append(res, getTablesFromMainTable(t)...)
	}
	return res
}
