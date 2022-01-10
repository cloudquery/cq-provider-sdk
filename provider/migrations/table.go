package migrations

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudquery/cq-provider-sdk/provider/migrations/longestcommon"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/hashicorp/go-hclog"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/thoas/go-funk"
)

const (
	queryTableColumns   = `SELECT array_agg(column_name::text) AS columns, array_agg(data_type::text) AS types FROM information_schema.columns WHERE table_name = $1`
	addColumnToTable    = `ALTER TABLE %s ADD COLUMN IF NOT EXISTS %v %v`
	dropColumnFromTable = `ALTER TABLE %s DROP COLUMN IF EXISTS %v`
	renameColumnInTable = `-- ALTER TABLE %s RENAME COLUMN %v TO %v; -- uncomment to activate, remove ADD/DROP COLUMN above and below` // Can't have IF EXISTS here

	dropTable = `DROP TABLE IF EXISTS %s`
)

// TableCreator handles creation of schema.Table in database as SQL strings
type TableCreator struct {
	log hclog.Logger
}

func NewTableCreator(log hclog.Logger) *TableCreator {
	return &TableCreator{
		log,
	}
}

// CreateTable generates CREATE TABLE definitions for the given table and runs them on the given conn
func (m TableCreator) CreateTable(ctx context.Context, conn *pgxpool.Conn, t, p *schema.Table) error {
	ups, _, err := m.CreateTableDefinitions(ctx, t, p)
	if err != nil {
		return err
	}
	for _, sql := range ups {
		if _, err := conn.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}

// CreateTableDefinitions reads schema.Table and builds the CREATE TABLE and DROP TABLE statements for it, also processing and returning subrelation tables
func (m TableCreator) CreateTableDefinitions(ctx context.Context, t *schema.Table, parent *schema.Table) (up, down []string, err error) {
	b := &strings.Builder{}

	// Build a SQL tocreate a table
	b.WriteString("CREATE TABLE IF NOT EXISTS " + strconv.Quote(t.Name) + " (\n")

	for _, c := range schema.GetDefaultSDKColumns() {
		b.WriteByte('\t')
		b.WriteString(strconv.Quote(c.Name) + ` ` + schema.GetPgTypeFromType(c.Type))
		if c.CreationOptions.Unique {
			b.WriteString(" UNIQUE")
		}
		b.WriteString(",\n")
	}

	m.buildColumns(b, t.Columns, parent)
	b.WriteString(fmt.Sprintf("\tCONSTRAINT %s_pk PRIMARY KEY(%s)\n", schema.TruncateTableConstraint(t.Name), strings.Join(t.PrimaryKeys(), ",")))
	b.WriteString(")")

	up, down = make([]string, 0, 1+len(t.Relations)), make([]string, 0, 1+len(t.Relations))
	up = append(up, b.String())

	// Create relation tables
	for _, r := range t.Relations {
		if cr, dr, err := m.CreateTableDefinitions(ctx, r, t); err != nil {
			return nil, nil, err
		} else {
			up = append(up, cr...)
			down = append(down, dr...)
		}
	}

	down = append(down, fmt.Sprintf(dropTable, t.Name))

	return up, down, nil
}

// UpgradeTable reads current table info from the given conn for the given table, and returns ALTER TABLE ADD COLUMN statements for the missing columns.
// Newly appearing tables will return a CREATE TABLE statement
// Column renamings are detected (best effort) and ALTER TABLE RENAME COLUMN statements are generated as comments.
// Table renamings or removals are not detected.
func (m TableCreator) UpgradeTable(ctx context.Context, conn *pgxpool.Conn, t, parent *schema.Table) (up, down []string, err error) {
	rows, err := conn.Query(ctx, queryTableColumns, t.Name)
	if err != nil {
		return nil, nil, err
	}

	var existingColumns struct {
		Columns []string
		Types   []string
	}

	if err := pgxscan.ScanOne(&existingColumns, rows); err != nil {
		return nil, nil, err
	}

	if len(existingColumns.Columns) == 0 {
		// Table does not exist, CREATE TABLE instead
		u, d, err := m.CreateTableDefinitions(ctx, t, parent)
		if err != nil {
			return nil, nil, fmt.Errorf("CreateTable: %w", err)
		}
		return u, d, nil
	}

	dbColTypes := make(map[string]string, len(existingColumns.Columns))
	for i := range existingColumns.Columns {
		dbColTypes[existingColumns.Columns[i]] = strings.ToLower(existingColumns.Types[i])
	}

	columnsToAdd, columnsToRemove := funk.DifferenceString(t.ColumnNames(), existingColumns.Columns)

	var similars map[string]string
	{
		upColsByType, downColsByType := make(map[string][]string), make(map[string][]string)

		for _, d := range columnsToAdd {
			col := t.Column(d)
			if col == nil {
				continue
			}
			upColsByType[schema.GetPgTypeFromType(col.Type)] = append(upColsByType[schema.GetPgTypeFromType(col.Type)], d)
		}
		for _, d := range columnsToRemove {
			if col := t.Column(d); col != nil {
				continue
			}
			downColsByType[dbColTypes[d]] = append(downColsByType[dbColTypes[d]], d)
		}

		similars = findSimilarColumnsWithSameType(upColsByType, downColsByType)
	}

	capSize := len(columnsToAdd) + len(columnsToRemove) // relations not included...
	up, down = make([]string, 0, capSize), make([]string, 0, capSize)
	downLast := make([]string, 0, capSize)

	for _, d := range columnsToAdd {
		m.log.Debug("adding column", "column", d)
		col := t.Column(d)
		if col == nil {
			m.log.Warn("column missing from table, not adding it", "table", t.Name, "column", d)
			continue
		}

		var notice string
		if v, ok := similars[d]; ok {
			notice = "; -- could this be " + strconv.Quote(v) + " ?"
		}

		up = append(up, fmt.Sprintf(addColumnToTable, strconv.Quote(t.Name), strconv.Quote(d), schema.GetPgTypeFromType(col.Type))+notice)
		downLast = append(downLast, fmt.Sprintf(dropColumnFromTable, strconv.Quote(t.Name), strconv.Quote(d))+notice)

		if v, ok := similars[d]; ok {
			up = append(up, fmt.Sprintf(renameColumnInTable, strconv.Quote(t.Name), strconv.Quote(v), strconv.Quote(d)))
			downLast = append(downLast, fmt.Sprintf(renameColumnInTable, strconv.Quote(t.Name), strconv.Quote(d), strconv.Quote(v)))
		}
	}

	for _, d := range columnsToRemove {
		m.log.Debug("removing column", "column", d)
		if col := t.Column(d); col != nil {
			m.log.Warn("column still in table, not removing it", "table", t.Name, "column", d)
			continue
		}

		var notice string
		if v, ok := similars[d]; ok {
			notice = "; -- could this be " + strconv.Quote(v) + " ? Check the RENAME COLUMN statement above"
		}

		up = append(up, fmt.Sprintf(dropColumnFromTable, strconv.Quote(t.Name), strconv.Quote(d))+notice)
		downLast = append(downLast, fmt.Sprintf(addColumnToTable, strconv.Quote(t.Name), strconv.Quote(d), dbColTypes[d])+notice)
	}

	// Do relation tables
	for _, r := range t.Relations {
		if cr, dr, err := m.UpgradeTable(ctx, conn, r, t); err != nil {
			return nil, nil, err
		} else {
			up = append(up, cr...)
			down = append(down, dr...)
		}
	}

	down = append(down, downLast...)

	return up, down, nil
}

func (m TableCreator) buildColumns(b *strings.Builder, cc []schema.Column, parent *schema.Table) {
	for _, c := range cc {
		b.WriteByte('\t')
		b.WriteString(strconv.Quote(c.Name) + " " + schema.GetPgTypeFromType(c.Type))
		if c.CreationOptions.Unique {
			b.WriteString(" UNIQUE")
		}
		if strings.HasSuffix(c.Name, "cq_id") && c.Name != "cq_id" {
			b.WriteString(" REFERENCES " + fmt.Sprintf("%s(cq_id)", parent.Name) + " ON DELETE CASCADE")
		}
		b.WriteString(",\n")
	}
}

func findSimilarColumnsWithSameType(setA, setB map[string][]string) map[string]string {
	const threshold = 4 // minimum common prefix/suffix length

	ret := make(map[string]string)

	for typeKey, alist := range setA {
		blist, ok := setB[typeKey]
		if !ok {
			continue
		}

		for _, A := range alist {
			for _, B := range blist {
				if A == B {
					panic("passed equal sets") // should not happen
				}

				pref := longestcommon.Prefix([]string{A, B})
				suf := longestcommon.Suffix([]string{A, B})
				if len(suf) > len(pref) {
					pref = suf
				}
				if len(pref) < threshold {
					continue
				}

				ret[A] = B
				ret[B] = A
			}
		}
	}

	return ret
}
