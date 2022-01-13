package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/modern-go/reflect2"
)

type DialectType string

const (
	Postgres = DialectType("postgres")
	TSDB     = DialectType("timescale")
)

func (t DialectType) MigrationDirectory() string {
	return string(t)
}

type Dialect interface {
	// PrimaryKeys returns the primary keys of table according to dialect
	PrimaryKeys(t *Table) []string

	// Columns returns the columns of table according to dialect
	Columns(t *Table) ColumnList

	// Constraints returns constraint definitions for table, according to dialect
	Constraints(t *Table) []string

	SupportsForeignKeys() bool

	// DBTypeFromType returns the database type from the given ValueType. Always lowercase.
	DBTypeFromType(v ValueType) string

	GetResourceValues(r *Resource) ([]interface{}, error)
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
	switch v {
	case TypeBool:
		return "boolean"
	case TypeInt:
		return "integer"
	case TypeBigInt:
		return "bigint"
	case TypeSmallInt:
		return "smallint"
	case TypeFloat:
		return "float"
	case TypeUUID:
		return "uuid"
	case TypeString:
		return "text"
	case TypeJSON:
		return "jsonb"
	case TypeIntArray:
		return "integer[]"
	case TypeStringArray:
		return "text[]"
	case TypeTimestamp:
		return "timestamp without time zone"
	case TypeByteArray:
		return "bytea"
	case TypeInvalid:
		fallthrough
	case TypeInet:
		return "inet"
	case TypeMacAddr:
		return "mac"
	case TypeInetArray:
		return "inet[]"
	case TypeMacAddrArray:
		return "mac[]"
	case TypeCIDR:
		return "cidr"
	case TypeCIDRArray:
		return "cidr[]"
	default:
		panic("invalid type")
	}
}

func (d PostgresDialect) SupportsForeignKeys() bool {
	return true
}

func (d PostgresDialect) GetResourceValues(r *Resource) ([]interface{}, error) {
	values := make([]interface{}, 0)
	for _, c := range append(r.table.Columns, GetDefaultSDKColumns()...) {
		v := r.Get(c.Name)
		if err := c.ValidateType(v); err != nil {
			return nil, err
		}
		if c.Type == TypeJSON {
			if v == nil {
				values = append(values, v)
				continue
			}
			if reflect2.TypeOf(v).Kind() == reflect.Map {
				values = append(values, v)
				continue
			}
			switch data := v.(type) {
			case map[string]interface{}:
				values = append(values, data)
			case string:
				newV := make(map[string]interface{})
				err := json.Unmarshal([]byte(data), &newV)
				if err != nil {
					return nil, err
				}
				values = append(values, newV)
			case *string:
				var newV interface{}
				err := json.Unmarshal([]byte(*data), &newV)
				if err != nil {
					return nil, err
				}
				values = append(values, newV)
			case []byte:
				var newV interface{}
				err := json.Unmarshal(data, &newV)
				if err != nil {
					return nil, err
				}
				values = append(values, newV)
			default:
				d, err := json.Marshal(data)
				if err != nil {
					return nil, err
				}
				var newV interface{}
				err = json.Unmarshal(d, &newV)
				if err != nil {
					return nil, err
				}
				values = append(values, newV)
			}
		} else {
			values = append(values, v)
		}
	}
	for _, v := range r.extraFields {
		values = append(values, v)
	}
	return values, nil
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
	return PostgresDialect{}.DBTypeFromType(v)
}

func (d TSDBDialect) GetResourceValues(r *Resource) ([]interface{}, error) {
	return PostgresDialect{}.GetResourceValues(r)
}

func (d TSDBDialect) SupportsForeignKeys() bool {
	return false
}

var (
	_ Dialect = (*PostgresDialect)(nil)
	_ Dialect = (*TSDBDialect)(nil)
)
