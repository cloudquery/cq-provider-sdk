package provider

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4"

	"github.com/huandu/go-sqlbuilder"

	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/hashicorp/go-hclog"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Migrator handles creation of schema.Table in database if they don't exist, and migration of tables if provider was upgraded.
type Migrator struct {
	log hclog.Logger
}

func NewMigrator(log hclog.Logger) *Migrator {
	return &Migrator{
		log,
	}
}

func (m Migrator) CreateTable(ctx context.Context, conn *pgxpool.Conn, t *schema.Table, parent *schema.Table) error {
	// Build a SQL to create a table.
	ctb := sqlbuilder.CreateTable(t.Name).IfNotExists()
	for _, c := range schema.GetDefaultSDKColumns() {
		if c.CreationOptions.Unique {
			ctb.Define(c.Name, schema.GetPgTypeFromType(c.Type), "unique")
		} else {
			ctb.Define(c.Name, schema.GetPgTypeFromType(c.Type))
		}
	}
	m.buildColumns(ctb, t.Columns, parent)
	ctb.Define(fmt.Sprintf("constraint %s_pk primary key(%s)", schema.TruncateTableConstraint(t.Name), strings.Join(t.PrimaryKeys(), ",")))
	sql, _ := ctb.BuildWithFlavor(sqlbuilder.PostgreSQL)
	if err := conn.BeginFunc(ctx, func(tx pgx.Tx) error {
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", strconv.Quote(t.Name))); err != nil {
			return err
		}
		m.log.Debug("creating table if not exists", "table", t.Name)
		if _, err := conn.Exec(ctx, sql); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if t.Relations == nil {
		return nil
	}

	m.log.Debug("creating table relations", "table", t.Name)
	// Create relation tables
	for _, r := range t.Relations {
		m.log.Debug("creating table relation", "table", r.Name)
		if err := m.CreateTable(ctx, conn, r, t); err != nil {
			return err
		}
	}
	return nil
}

func (m Migrator) buildColumns(ctb *sqlbuilder.CreateTableBuilder, cc []schema.Column, parent *schema.Table) {
	for _, c := range cc {
		defs := []string{strconv.Quote(c.Name), schema.GetPgTypeFromType(c.Type)}
		if c.CreationOptions.Unique {
			defs = []string{strconv.Quote(c.Name), schema.GetPgTypeFromType(c.Type), "unique"}
		}
		if strings.HasSuffix(c.Name, "cq_id") && c.Name != "cq_id" {
			defs = append(defs, "REFERENCES", fmt.Sprintf("%s(cq_id)", parent.Name), "ON DELETE CASCADE")
		}
		ctb.Define(defs...)
	}
}
