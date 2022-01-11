package schema

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type DialectType string

const (
	Postgres = DialectType("postgres")
	TSDB     = DialectType("timescale")
)

type Dialect interface {
	// DefaultSDKColumns returns default columns of the SDK, these columns are added to each table by default
	//DefaultSDKColumns() []Column

	// PrimaryKeys returns the primary keys of table according to dialect
	PrimaryKeys(t *Table) []string

	// Columns returns the columns of table according to dialect
	Columns(t *Table) []Column

	// Constraints returns constraint definitions for table, according to dialect
	Constraints(t *Table) []string

	// GenerateMigration generates up and down migrations for the table
	GenerateMigration(t, parent *Table) (*Migration, error)
}

type Migration struct {
	Up, Down []string
}

func (m *Migration) Add(up, down []string) {
	m.Up = append(m.Up, up...)
	m.Down = append(m.Down, down...)
}

func (m *Migration) Ups() []byte {
	return m.toBytes(m.Up)
}

func (m *Migration) Downs() []byte {
	return m.toBytes(m.Down)
}

func (m *Migration) toBytes(s []string) []byte {
	b := &bytes.Buffer{}
	for i := range s {
		b.WriteString(s[i])
		if !strings.HasPrefix(s[i], "--") {
			b.WriteByte(';')
		}
		b.WriteByte('\n')
	}
	return b.Bytes()
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

func (d PostgresDialect) Columns(t *Table) []Column {
	return append([]Column{cqIdColumn, cqMeta}, t.Columns...)
}

func (d PostgresDialect) Constraints(t *Table) []string {
	ret := make([]string, 0, len(t.Columns))

	ret = append(ret, fmt.Sprintf("CONSTRAINT %s_pk PRIMARY KEY(%s)", TruncateTableConstraint(t.Name), strings.Join(d.PrimaryKeys(t), ",")))

	for _, c := range t.Columns {
		if !c.CreationOptions.Unique {
			continue
		}

		ret = append(ret, fmt.Sprintf("UNIQUE(%s)", c.Name))
	}

	return ret
}

func (d PostgresDialect) GenerateMigration(t, parent *Table) (*Migration, error) {
	b := &strings.Builder{}

	// Build a SQL to create a table
	b.WriteString("CREATE TABLE IF NOT EXISTS " + strconv.Quote(t.Name) + " (\n")

	for _, c := range t.Columns {
		b.WriteByte('\t')
		b.WriteString(strconv.Quote(c.Name) + " " + GetPgTypeFromType(c.Type))
		if c.CreationOptions.NotNull {
			b.WriteString(" NOT NULL")
		}
		// c.CreationOptions.Unique is handled in the Constraints() call below
		if parent != nil && strings.HasSuffix(c.Name, "cq_id") && c.Name != "cq_id" {
			b.WriteString(" REFERENCES " + fmt.Sprintf("%s(cq_id)", parent.Name) + " ON DELETE CASCADE")
		}
		b.WriteString(",\n")
	}

	for _, cn := range d.Constraints(t) {
		b.WriteByte('\t')
		b.WriteString(cn)
		b.WriteByte('\n')
	}

	b.WriteString(")")

	ret := &Migration{}
	ret.Add([]string{
		"-- Resource: " + t.Name,
		b.String(),
	}, nil)

	// Create relation tables
	for _, r := range t.Relations {
		relmig, err := d.GenerateMigration(r, t)
		if err != nil {
			return nil, err
		}
		ret.Add(relmig.Up, relmig.Down)
	}

	ret.Add(nil, []string{fmt.Sprintf("DROP TABLE IF EXISTS %s", t.Name)})
	return ret, nil
}

//func (d PostgresDialect) defaultSDKColumns() []Column {
//	return []Column{cqIdColumn, cqMeta}
//}

type TSDBDialect struct{}

func (d TSDBDialect) PrimaryKeys(t *Table) []string {
	v := PostgresDialect{}.PrimaryKeys(t)
	return append([]string{cqFetchDateColumn.Name}, v...)
}

func (d TSDBDialect) Columns(t *Table) []Column {
	return append([]Column{cqIdColumn, cqMeta, cqFetchDateColumn}, t.Columns...)
}

func (d TSDBDialect) Constraints(t *Table) []string {
	ret := make([]string, 0, len(t.Columns))

	ret = append(ret, fmt.Sprintf("CONSTRAINT %s_pk PRIMARY KEY(%s)", TruncateTableConstraint(t.Name), strings.Join(d.PrimaryKeys(t), ",")))

	for _, c := range t.Columns {
		if !c.CreationOptions.Unique {
			continue
		}

		ret = append(ret, fmt.Sprintf("UNIQUE(%s,%s)", cqFetchDateColumn.Name, c.Name))
	}

	return ret
}

func (d TSDBDialect) GenerateMigration(t, _ *Table) (*Migration, error) {
	b := &strings.Builder{}

	// Build a SQL to create a table
	b.WriteString("CREATE TABLE IF NOT EXISTS " + strconv.Quote(t.Name) + " (\n")

	for _, c := range t.Columns {
		b.WriteByte('\t')
		b.WriteString(strconv.Quote(c.Name) + " " + GetPgTypeFromType(c.Type))
		if c.CreationOptions.NotNull {
			b.WriteString(" NOT NULL")
		}
		// c.CreationOptions.Unique is handled in the Constraints() call below
		// No FK things
		b.WriteString(",\n")
	}

	for _, cn := range d.Constraints(t) {
		b.WriteByte('\t')
		b.WriteString(cn)
		b.WriteByte('\n')
	}

	b.WriteString(")")

	ret := &Migration{}
	ret.Add([]string{
		"-- Resource: " + t.Name,
		b.String(),
	}, nil)

	// Create relation tables
	for _, r := range t.Relations {
		relmig, err := d.GenerateMigration(r, t)
		if err != nil {
			return nil, err
		}
		ret.Add(relmig.Up, relmig.Down)
	}

	ret.Add(nil, []string{fmt.Sprintf("DROP TABLE IF EXISTS %s", t.Name)})
	return ret, nil
}

//func (d TSDBDialect) defaultSDKColumns() []Column {
//	return []Column{cqIdColumn, cqMeta, cqFetchDateColumn}
//}

var (
	_ Dialect = (*PostgresDialect)(nil)
	_ Dialect = (*TSDBDialect)(nil)
)
