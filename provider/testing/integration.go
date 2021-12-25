package testing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/cloudquery/cq-provider-sdk/logging"
	"github.com/cloudquery/cq-provider-sdk/provider"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/cloudquery/cq-provider-sdk/provider/testing/sqldiff"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/hashicorp/go-hclog"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
)

type ResourceIntegrationTestData struct {
	ProviderCreator func() *provider.Provider
	Table           *schema.Table
	Config          string
	SnapshotsDir    string
}

// IntegrationTest - creates resources using terraform, fetches them to db and compares with expected values
func IntegrationTest(t *testing.T, resource ResourceIntegrationTestData) {
	t.Parallel()
	t.Helper()
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

	if err := deleteTables(conn, resource.Table); err != nil {
		t.Fatal(err)
	}

	if err = fetch(&resource); err != nil {
		t.Fatal(err)
	}

	equal, err := verifyTable(t, conn, resource.Table, resource)
	if err != nil {
		t.Fatal(err)
	}
	if !equal {
		t.Error("results not equal")
	}

	if err := conn.Conn().Close(ctx); err != nil {
		t.Fatal(err)
	}

}

// fetch - fetches resources from the cloud and puts them into database. database config can be specified via DATABASE_URL env variable
func fetch(resource *ResourceIntegrationTestData) error {
	log.Printf("%s fetch resources\n", resource.Table.Name)
	testProvider := resource.ProviderCreator()
	testProvider.Logger = logging.New(hclog.DefaultOptions)

	if _, err := testProvider.ConfigureProvider(context.Background(), &cqproto.ConfigureProviderRequest{
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

	if err := testProvider.FetchResources(context.Background(),
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

func verifyTable(t *testing.T, conn *pgxpool.Conn, table *schema.Table, resource ResourceIntegrationTestData) (bool, error) {
	t.Helper()
	res := true
	// the order of insertion is hopefully the same (depends on api. AWS Im looking at you...)
	// this is why we order by meta->>last_updated
	// Future note - if api will return results not in the same order we will have to do a smarter diff that doesn't rely on order
	// this is not hard but just will provider worse debug info on what is changed
	s := sq.StatementBuilder.
		PlaceholderFormat(sq.Dollar).
		Select(fmt.Sprintf("json_agg(%s order by meta->>'last_updated')", table.Name)).
		From(table.Name)

	query, args, err := s.ToSql()
	if err != nil {
		return false, err
	}

	var data []map[string]interface{}
	if err := pgxscan.Get(context.Background(), conn, &data, query, args...); err != nil {
		return false, err
	}

	b, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return false, err
	}

	snapshotPath := path.Join(resource.SnapshotsDir, table.Name+".snapshot")

	if _, err := os.Stat(snapshotPath); err == nil {
		// snapshot already exist check if content is equal, if not fail
		snapshotContent, err := os.ReadFile(snapshotPath)
		if err != nil {
			return false, err
		}
		var savedData []map[string]interface{}
		json.Unmarshal(snapshotContent, &savedData)
		d := sqldiff.New([]string{})
		diff := d.CompareTwoResults(data, savedData)
		if len(diff) != 0 {
			t.Log("found diff")
			for _, v := range diff {
				t.Log(v)
			}
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
