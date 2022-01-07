package migrations

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/hashicorp/go-hclog"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/thoas/go-funk"
)

const (
	queryTableColumns   = `SELECT array_agg(column_name::text) AS columns FROM information_schema.columns WHERE table_name = $1`
	addColumnToTable    = `ALTER TABLE %s ADD COLUMN IF NOT EXISTS %v %v`
	dropColumnFromTable = `ALTER TABLE %s DROP COLUMN IF EXISTS %v`

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

// CreateTable reads schema.Table and builds the CREATE TABLE and DROP TABLE statements for it, also processing and returning subrelation tables
func (m TableCreator) CreateTable(ctx context.Context, t *schema.Table, parent *schema.Table) (up, down []string, err error) {
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
		if cr, dr, err := m.CreateTable(ctx, r, t); err != nil {
			return nil, nil, err
		} else {
			up = append(up, cr...)
			down = append(down, dr...)
		}
	}

	down = append(down, fmt.Sprintf(dropTable, t.Name))

	return up, down, nil
}

// UpgradeTable reads current table info from the given conn for the given table, and returns ALTER TABLE ADD COLUMN statements for the missing columns
func (m TableCreator) UpgradeTable(ctx context.Context, conn *pgxpool.Conn, t *schema.Table) (up, down []string, err error) {
	rows, err := conn.Query(ctx, queryTableColumns, t.Name)
	if err != nil {
		return nil, nil, err
	}

	var existingColumns struct {
		Columns []string
	}

	if err := pgxscan.ScanOne(&existingColumns, rows); err != nil {
		return nil, nil, err
	}

	columnsToAdd, columnsToRemove := funk.DifferenceString(t.ColumnNames(), existingColumns.Columns)

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

		up = append(up, fmt.Sprintf(addColumnToTable, strconv.Quote(t.Name), strconv.Quote(d), schema.GetPgTypeFromType(col.Type)))
		downLast = append(downLast, fmt.Sprintf(dropColumnFromTable, strconv.Quote(t.Name), strconv.Quote(d)))
	}

	for _, d := range columnsToRemove {
		m.log.Debug("removing column", "column", d)
		if col := t.Column(d); col != nil {
			m.log.Warn("column still in table, not removing it", "table", t.Name, "column", d)
			continue
		}

		up = append(up, fmt.Sprintf(dropColumnFromTable, strconv.Quote(t.Name), strconv.Quote(d)))
		downLast = append(downLast, fmt.Sprintf(addColumnToTable, strconv.Quote(t.Name), strconv.Quote(d), "WHATS THE TYPE"))
	}

	// Do relation tables
	for _, r := range t.Relations {
		if cr, dr, err := m.UpgradeTable(ctx, conn, r); err != nil {
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
