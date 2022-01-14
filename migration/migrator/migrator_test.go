package migrator

import (
	"context"
	"testing"

	"github.com/cloudquery/cq-provider-sdk/helpers"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"

	"github.com/golang-migrate/migrate/v4"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
)

const (
	defaultQuery = "select 1;"
	emptyQuery   = ""
)

var (
	simpleMigrations = map[string][]byte{
		"postgres/1_v0.0.1.up.sql":        []byte(defaultQuery),
		"postgres/1_v0.0.1.down.sql":      []byte(defaultQuery),
		"postgres/3_v0.0.2.up.sql":        []byte(defaultQuery),
		"postgres/3_v0.0.2.down.sql":      []byte(defaultQuery),
		"postgres/2_v0.0.2-beta.up.sql":   []byte(defaultQuery),
		"postgres/2_v0.0.2-beta.down.sql": []byte(defaultQuery),
		"postgres/4_v0.0.3.up.sql":        []byte(defaultQuery),
		"postgres/4_v0.0.3.down.sql":      []byte(defaultQuery),
		"postgres/5_v0.0.4.up.sql":        []byte(emptyQuery),
		"postgres/5_v0.0.4.down.sql":      []byte(defaultQuery),
	}

	complexMigrations = map[string][]byte{
		"postgres/1_v0.0.2.up.sql":        []byte(defaultQuery),
		"postgres/1_v0.0.2.down.sql":      []byte(defaultQuery),
		"postgres/2_v0.0.3-beta.up.sql":   []byte(defaultQuery),
		"postgres/2_v0.0.3-beta.down.sql": []byte(defaultQuery),
		"postgres/3_v0.0.3.up.sql":        []byte(defaultQuery),
		"postgres/3_v0.0.3.down.sql":      []byte(defaultQuery),
		"postgres/4_v0.0.6.up.sql":        []byte(defaultQuery),
		"postgres/4_v0.0.6.down.sql":      []byte(defaultQuery),
		"postgres/5_v0.1.4.up.sql":        []byte(emptyQuery),
		"postgres/5_v0.1.4.down.sql":      []byte(defaultQuery),
	}
)

func TestMigrations(t *testing.T) {
	m, err := New(hclog.Default(), schema.Postgres, simpleMigrations, "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable", "test", nil)
	assert.Nil(t, err)

	err = m.DropProvider(context.Background(), nil)
	assert.Nil(t, err)

	err = m.UpgradeProvider("latest")
	assert.Nil(t, err)

	err = m.UpgradeProvider("latest")
	assert.Equal(t, err, migrate.ErrNoChange)

	err = m.DowngradeProvider("v0.0.2-beta")
	assert.Nil(t, err)

	err = m.UpgradeProvider("v0.0.3")
	assert.Nil(t, err)

	version, dirty, err := m.Version()
	assert.Equal(t, []interface{}{"v0.0.3", false, nil}, []interface{}{version, dirty, err})

	err = m.UpgradeProvider("latest")
	assert.Nil(t, err)

	version, dirty, err = m.Version()
	assert.Equal(t, []interface{}{"v0.0.4", false, nil}, []interface{}{version, dirty, err})

	err = m.UpgradeProvider("v0.0.4")
	assert.Equal(t, err, migrate.ErrNoChange)

	version, dirty, err = m.Version()
	assert.Equal(t, []interface{}{"v0.0.4", false, nil}, []interface{}{version, dirty, err})
}

// TestMigrationJumps tests an edge case we request a higher version but latest migration is a previous version
func TestMigrationJumps(t *testing.T) {
	m, err := New(hclog.Default(), schema.Postgres, complexMigrations, "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable", "test", nil)
	assert.Nil(t, err)

	err = m.DropProvider(context.Background(), nil)
	assert.Nil(t, err)

	err = m.UpgradeProvider("v0.2.0")
	assert.Nil(t, err)

	version, dirty, err := m.Version()
	assert.Equal(t, []interface{}{"v0.1.4", false, nil}, []interface{}{version, dirty, err})
}

func TestMultiProviderMigrations(t *testing.T) {
	mtest, err := New(hclog.Default(), schema.Postgres, simpleMigrations, "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable", "test", nil)
	assert.Nil(t, err)

	mtest2, err := New(hclog.Default(), schema.Postgres, simpleMigrations, "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable", "test2", nil)
	assert.Nil(t, err)

	err = mtest.DropProvider(context.Background(), nil)
	assert.Nil(t, err)
	err = mtest2.DropProvider(context.Background(), nil)
	assert.Nil(t, err)

	err = mtest.UpgradeProvider("latest")
	assert.Nil(t, err)
	err = mtest.UpgradeProvider("latest")
	assert.Equal(t, err, migrate.ErrNoChange)
	version, dirty, err := mtest.Version()
	assert.Equal(t, []interface{}{"v0.0.4", false, nil}, []interface{}{version, dirty, err})

	version, dirty, err = mtest2.Version()
	assert.Equal(t, []interface{}{"v0.0.0", false, migrate.ErrNilVersion}, []interface{}{version, dirty, err})
	err = mtest2.UpgradeProvider("v0.0.3")
	assert.Nil(t, err)
	version, dirty, err = mtest2.Version()
	assert.Equal(t, []interface{}{"v0.0.3", false, nil}, []interface{}{version, dirty, err})

	err = mtest.DropProvider(context.Background(), nil)
	assert.Nil(t, err)

	version, dirty, err = mtest2.Version()
	assert.Equal(t, []interface{}{"v0.0.3", false, nil}, []interface{}{version, dirty, err})
	version, dirty, err = mtest.Version()
	assert.Equal(t, []interface{}{"v0.0.0", false, migrate.ErrNilVersion}, []interface{}{version, dirty, err})
}

func TestFindLatestMigration(t *testing.T) {
	mtest, err := New(hclog.Default(), schema.Postgres, complexMigrations, "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable", "test", nil)
	assert.Nil(t, err)
	mv, err := mtest.FindLatestMigration("v0.0.3")
	assert.Nil(t, err)
	assert.Equal(t, uint(3), mv)

	mv, err = mtest.FindLatestMigration("v0.0.3-alpha")
	assert.Nil(t, err)
	assert.Equal(t, uint(1), mv)

	mv, err = mtest.FindLatestMigration("v0.1.3-alpha")
	assert.Nil(t, err)
	assert.Equal(t, uint(4), mv)

	mv, err = mtest.FindLatestMigration("v0.1.5")
	assert.Nil(t, err)
	assert.Equal(t, uint(5), mv)

	mv, err = mtest.FindLatestMigration("v0.0.1")
	assert.Nil(t, err)
	assert.Equal(t, uint(0), mv)

	mv, err = mtest.FindLatestMigration("latest")
	assert.Nil(t, err)
	assert.Equal(t, uint(5), mv)
}

func TestParseConnectionString(t *testing.T) {
	url, err := helpers.ParseConnectionString("postgres://postgres:pass@localhost:5432/postgres?sslmode=disable")
	assert.Nil(t, err)
	assert.Equal(t, "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable", url.String())
	url, err = helpers.ParseConnectionString("host=localhost user=postgres password=pass database=postgres port=5432 sslmode=disable")
	assert.Nil(t, err)
	assert.Equal(t, "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable", url.String())
}