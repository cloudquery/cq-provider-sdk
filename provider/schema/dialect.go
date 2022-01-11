package schema

import (
	"fmt"
	"strings"
)

type DialectType string

const (
	Postgres = DialectType("postgres")
	TSDB     = DialectType("timescale")
)

type Dialect interface {
	// PrimaryKeys returns the primary keys of table according to dialect
	PrimaryKeys(t *Table) []string

	// Columns returns the columns of table according to dialect
	Columns(t *Table) ColumnList

	// Constraints returns constraint definitions for table, according to dialect
	Constraints(t *Table) []string

	// DBTypeFromType returns the database type from the given ValueType
	DBTypeFromType(v ValueType) string

	SupportsForeignKeys() bool
}

func GetDialect(t DialectType) Dialect {
	switch t {
	case Postgres:
		return PostgresDialect{}
	case TSDB:
		return TSDBDialect{}
	default:
		return nil
	}
}

type PostgresDialect struct{}

func (d PostgresDialect) PrimaryKeys(t *Table) []string {
	if len(t.Options.PrimaryKeys) > 0 {
		return t.Options.PrimaryKeys
	}
	return []string{cqIdColumn.Name}
}

func (d PostgresDialect) Columns(t *Table) ColumnList {
	return append([]Column{cqIdColumn, cqMeta}, t.Columns...)
}

func (d PostgresDialect) Constraints(t *Table) []string {
	ret := make([]string, 0, len(t.Columns))

	ret = append(ret, fmt.Sprintf("CONSTRAINT %s_pk PRIMARY KEY(%s)", TruncateTableConstraint(t.Name), strings.Join(d.PrimaryKeys(t), ",")))

	for _, c := range d.Columns(t) {
		if !c.CreationOptions.Unique {
			continue
		}

		ret = append(ret, fmt.Sprintf("UNIQUE(%s)", c.Name))
	}

	return ret
}

func (d PostgresDialect) DBTypeFromType(v ValueType) string {
	return GetPgTypeFromType(v)
}

func (d PostgresDialect) SupportsForeignKeys() bool {
	return true
}

type TSDBDialect struct{}

func (d TSDBDialect) PrimaryKeys(t *Table) []string {
	v := PostgresDialect{}.PrimaryKeys(t)
	return append([]string{cqFetchDateColumn.Name}, v...)
}

func (d TSDBDialect) Columns(t *Table) ColumnList {
	return append([]Column{cqIdColumn, cqMeta, cqFetchDateColumn}, t.Columns...)
}

func (d TSDBDialect) Constraints(t *Table) []string {
	ret := make([]string, 0, len(t.Columns))

	ret = append(ret, fmt.Sprintf("CONSTRAINT %s_pk PRIMARY KEY(%s)", TruncateTableConstraint(t.Name), strings.Join(d.PrimaryKeys(t), ",")))

	for _, c := range d.Columns(t) {
		if !c.CreationOptions.Unique {
			continue
		}

		ret = append(ret, fmt.Sprintf("UNIQUE(%s,%s)", cqFetchDateColumn.Name, c.Name))
	}

	return ret
}

func (d TSDBDialect) DBTypeFromType(v ValueType) string {
	return GetPgTypeFromType(v)
}

func (d TSDBDialect) SupportsForeignKeys() bool {
	return false
}

var (
	_ Dialect = (*PostgresDialect)(nil)
	_ Dialect = (*TSDBDialect)(nil)
)
