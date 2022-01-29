package execution

import (
	"context"
	"time"

	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/jackc/pgx/v4"
)

type doNothingStorage struct {
}

func (f doNothingStorage) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	return nil, nil
}

func (f doNothingStorage) Exec(ctx context.Context, query string, args ...interface{}) error {
	return nil
}

func (f doNothingStorage) Insert(ctx context.Context, t *schema.Table, instance schema.Resources) error {
	return nil
}

func (f doNothingStorage) Delete(ctx context.Context, t *schema.Table, kvFilters []interface{}) error {
	return nil
}

func (f doNothingStorage) RemoveStaleData(ctx context.Context, t *schema.Table, executionStart time.Time, kvFilters []interface{}) error {
	return nil
}

func (f doNothingStorage) CopyFrom(ctx context.Context, resources schema.Resources, shouldCascade bool, CascadeDeleteFilters map[string]interface{}) error {
	return nil
}

func (f doNothingStorage) Close() {}

func (f doNothingStorage) Dialect() schema.Dialect {
	return doNothingDialect{}
}

type doNothingDialect struct {
}

func (d doNothingDialect) PrimaryKeys(t *schema.Table) []string {
	return t.Options.PrimaryKeys
}

func (d doNothingDialect) Columns(t *schema.Table) schema.ColumnList {
	return t.Columns
}

func (d doNothingDialect) Constraints(t, parent *schema.Table) []string {
	return []string{}
}

func (d doNothingDialect) Extra(t, parent *schema.Table) []string {
	return []string{}
}

func (d doNothingDialect) DBTypeFromType(v schema.ValueType) string {
	return v.String()
}

func (d doNothingDialect) GetResourceValues(r *schema.Resource) ([]interface{}, error) {
	return r.Values()
}
