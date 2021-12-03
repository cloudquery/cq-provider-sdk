package testing

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/cloudquery/cq-provider-sdk/logging"
	"github.com/cloudquery/cq-provider-sdk/provider"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/cloudquery/faker/v3"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmccombs/hcl2json/convert"
)

type ResourceTestData struct {
	Table          *schema.Table
	Config         interface{}
	Resources      []string
	Configure      func(logger hclog.Logger, data interface{}) (schema.ClientMeta, error)
	SkipEmptyJsonB bool
}

func TestResource(t *testing.T, providerCreator func() *provider.Provider, resource ResourceTestData) {
	if err := faker.SetRandomMapAndSliceMinSize(1); err != nil {
		t.Fatal(err)
	}
	if err := faker.SetRandomMapAndSliceMaxSize(1); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	pool, err := setupDatabase()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	l := logging.New(hclog.DefaultOptions)
	migrator := provider.NewTableCreator(l)
	if err := migrator.CreateTable(ctx, conn, resource.Table, nil); err != nil {
		assert.FailNow(t, fmt.Sprintf("failed to create tables %s", resource.Table.Name), err)
	}
	// Write configuration as a block and extract it out passing that specific block data as part of the configure provider
	f := hclwrite.NewFile()
	f.Body().AppendBlock(gohcl.EncodeAsBlock(resource.Config, "configuration"))
	data, err := convert.Bytes(f.Bytes(), "config.json", convert.Options{})
	require.Nil(t, err)
	hack := map[string]interface{}{}
	require.Nil(t, json.Unmarshal(data, &hack))
	data, err = json.Marshal(hack["configuration"].([]interface{})[0])
	require.Nil(t, err)

	testProvider := providerCreator()
	testProvider.Logger = l
	testProvider.Configure = resource.Configure
	_, err = testProvider.ConfigureProvider(context.Background(), &cqproto.ConfigureProviderRequest{
		CloudQueryVersion: "",
		Connection: cqproto.ConnectionDetails{DSN: getEnv("DATABASE_URL",
			"host=localhost user=postgres password=pass DB.name=postgres port=5432")},
		Config: data,
	})
	assert.Nil(t, err)

	err = testProvider.FetchResources(context.Background(), &cqproto.FetchResourcesRequest{Resources: []string{findResourceFromTableName(resource.Table, testProvider.ResourceMap)}}, &fakeResourceSender{Errors: []string{}})
	assert.Nil(t, err)
	sequence := getVerificationSequence(resource.Table)
	verifyColumnsBySequence(t, conn, sequence, nil)
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

func setupDatabase() (*pgxpool.Pool, error) {
	dbCfg, err := pgxpool.ParseConfig(getEnv("DATABASE_URL",
		"host=localhost user=postgres password=pass DB.name=postgres port=5432"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse config. %w", err)
	}
	ctx := context.Background()
	dbCfg.MaxConns = 1
	dbCfg.LazyConnect = true
	pool, err := pgxpool.ConnectConfig(ctx, dbCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database. %w", err)
	}
	return pool, nil

}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

type VerificationSequence struct {
	TableName      string
	ForeignKeyName string
	Relations      []VerificationSequence
}

func getVerificationSequence(table *schema.Table) VerificationSequence {
	res := VerificationSequence{
		TableName: table.Name,
	}
	for _, c := range table.Columns {
		if strings.Contains(c.Name, "_cq_id") {
			res.ForeignKeyName = c.Name
		}
	}
	for _, t := range table.Relations {
		res.Relations = append(res.Relations, getVerificationSequence(t))
	}
	return res
}
func verifyColumnsBySequence(t *testing.T, conn pgxscan.Querier, sequence VerificationSequence, parentID *string) {
	sq := squirrel.StatementBuilder.
		PlaceholderFormat(squirrel.Dollar).
		Select(fmt.Sprintf("json_agg(%s)", sequence.TableName)).
		From(sequence.TableName)
	if parentID == nil {
		// it is a root object
		sq = sq.Where(squirrel.Eq{"account_id": "testAccount"})
	} else {
		// it is a relation
		sq = sq.Where(squirrel.Eq{sequence.ForeignKeyName: parentID})
	}
	query, args, err := sq.ToSql()
	if err != nil {
		t.Fatalf("Failed to build query for %s:%v", sequence.TableName, err)
	}
	var data []map[string]interface{}
	if err = pgxscan.Get(context.Background(), conn, &data, query, args...); err != nil {
		t.Fatalf("Failed to execute query to %s:%v", sequence.TableName, err)
	}
	if len(data) == 0 {
		t.Fatalf("expected to have at least 1 entry at table %s got %d", sequence.TableName, len(data))
	}

	var pID *string
	for _, d := range data {
		var nullColumns []string
		for k, v := range d {
			if v == nil {
				nullColumns = append(nullColumns, k)
				continue
			}
			if k == "cq_id" {
				id, ok := v.(string)
				if !ok {
					t.Fatalf("table %s column %s cq_id column has wrong type", sequence.TableName, k)
				}
				pID = &id
			}
		}
		if len(nullColumns) > 0 {
			t.Fatalf("table '%s' has NULL coumns: '%s'", sequence.TableName, strings.Join(nullColumns, "', '"))
		}
	}
	for _, r := range sequence.Relations {
		verifyColumnsBySequence(t, conn, r, pID)
	}
}
