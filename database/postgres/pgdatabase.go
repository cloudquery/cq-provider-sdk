package postgres

import (
	"context"
	"fmt"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/cloudquery/cq-provider-sdk/provider/schema/diag"
	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/hashicorp/go-hclog"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/cast"
)

type PgDatabase struct {
	pool *pgxpool.Pool
	log  hclog.Logger
	sd   schema.Dialect
}

func NewPgDatabase(ctx context.Context, logger hclog.Logger, dsn string, sd schema.Dialect) (*PgDatabase, error) {
	pool, err := Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &PgDatabase{
		pool: pool,
		log:  logger,
		sd:   sd,
	}, nil
}

var _ schema.Database = (*PgDatabase)(nil)

// Insert inserts all resources to given table, table and resources are assumed from same table.
func (p PgDatabase) Insert(ctx context.Context, t *schema.Table, resources schema.Resources) error {
	if len(resources) == 0 {
		return nil
	}
	// It is safe to assume that all resources have the same columns
	cols := quoteColumns(resources.ColumnNames())
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	sqlStmt := psql.Insert(t.Name).Columns(cols...)
	for _, res := range resources {
		if res.TableName() != t.Name {
			return fmt.Errorf("resource table expected %s got %s", t.Name, res.TableName())
		}
		values, err := res.Values()
		if err != nil {
			return fmt.Errorf("table %s insert failed %w", t.Name, err)
		}
		sqlStmt = sqlStmt.Values(values...)
	}

	s, args, err := sqlStmt.ToSql()
	if err != nil {
		return diag.FromError(err, diag.ERROR, diag.DATABASE, t.Name, "bad insert SQL statement created", "")
	}
	_, err = p.pool.Exec(ctx, s, args...)
	if err == nil {
		return nil
	}
	if pgErr, ok := err.(*pgconn.PgError); ok {
		// This should rarely occur, but if it occurs we want to print the SQL to debug it further
		if pgerrcode.IsSyntaxErrororAccessRuleViolation(pgErr.Code) {
			p.log.Debug("insert syntax error", "sql", s)
		}
		if pgerrcode.IsIntegrityConstraintViolation(pgErr.Code) {
			p.log.Debug("insert integrity violation error", "constraint", pgErr.ConstraintName, "errMsg", pgErr.Message)
		}
		return diag.FromError(err, diag.ERROR, diag.DATABASE, t.Name, fmt.Sprintf("insert failed for table %s", t.Name), pgErr.Message)
	}
	return diag.FromError(err, diag.ERROR, diag.DATABASE, t.Name, err.Error(), "")
}

// CopyFrom copies all resources from []*Resource
func (p PgDatabase) CopyFrom(ctx context.Context, resources schema.Resources, shouldCascade bool, cascadeDeleteFilters map[string]interface{}) error {
	if len(resources) == 0 {
		return nil
	}
	err := p.pool.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		if shouldCascade {
			q := goqu.Dialect("postgres").Delete(resources.TableName()).Where(goqu.Ex{"cq_id": resources.GetIds()})
			for k, v := range cascadeDeleteFilters {
				q = q.Where(goqu.Ex{k: goqu.Op{"eq": v}})
			}
			sql, args, err := q.Prepared(true).ToSQL()
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, sql, args...)
			if err != nil {
				return err
			}
		}
		copied, err := tx.CopyFrom(
			ctx, pgx.Identifier{resources.TableName()}, resources.ColumnNames(),
			pgx.CopyFromSlice(len(resources), func(i int) ([]interface{}, error) {
				// use getResourceValues instead of Resource.Values since values require some special encoding for CopyFrom
				return p.sd.GetResourceValues(resources[i])
			}))
		if err != nil {
			return err
		}
		if copied != int64(len(resources)) {
			return fmt.Errorf("not all resources copied %d != %d to %s", copied, len(resources), resources.TableName())
		}
		return nil
	})
	return err
}

// Exec allows executions of postgres queries with given args returning error of execution
func (p PgDatabase) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := p.pool.Exec(ctx, query, args...)
	return err
}

// Query  allows execution of postgres queries with given args returning data result
func (p PgDatabase) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	return rows, err
}

// QueryOne  allows execution of postgres queries with given args returning data result
func (p PgDatabase) QueryOne(ctx context.Context, query string, args ...interface{}) pgx.Row {
	row := p.pool.QueryRow(ctx, query, args...)
	return row
}

func (p PgDatabase) Delete(ctx context.Context, t *schema.Table, kvFilters []interface{}) error {
	nc := len(kvFilters)
	if nc%2 != 0 {
		return fmt.Errorf("number of args to delete should be even. Got %d", nc)
	}
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	ds := psql.Delete(t.Name)
	for i := 0; i < nc; i += 2 {
		ds = ds.Where(sq.Eq{kvFilters[i].(string): kvFilters[i+1]})
	}
	sql, args, err := ds.ToSql()
	if err != nil {
		return err
	}

	_, err = p.pool.Exec(ctx, sql, args...)
	return err
}

func (p PgDatabase) RemoveStaleData(ctx context.Context, t *schema.Table, executionStart time.Time, kvFilters []interface{}) error {
	q := goqu.Delete(t.Name).WithDialect("postgres").Where(goqu.L(`extract(epoch from (meta->>'last_updated')::timestamp)`).Lt(executionStart.Unix()))
	if len(kvFilters)%2 != 0 {
		return fmt.Errorf("expected even number of k,v delete filters received %s", kvFilters)
	}
	for i := 0; i < len(kvFilters); i += 2 {
		q = q.Where(goqu.Ex{cast.ToString(kvFilters[i]): goqu.Op{"eq": kvFilters[i+1]}})
	}
	sql, args, err := q.Prepared(true).ToSQL()
	if err != nil {
		return fmt.Errorf("failed building query: %w", err)
	}
	_, err = p.pool.Exec(ctx, sql, args...)
	return err
}

func (p PgDatabase) Close() {
	p.pool.Close()
}

func (p PgDatabase) Dialect() schema.Dialect {
	return p.sd
}

func quoteColumns(columns []string) []string {
	for i, v := range columns {
		columns[i] = strconv.Quote(v)
	}
	return columns
}
