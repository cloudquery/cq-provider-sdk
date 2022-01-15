package database

import (
	"context"

	"github.com/cloudquery/cq-provider-sdk/database/postgres"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/hashicorp/go-hclog"
)

type DB struct {
	schema.Database

	dialectType schema.DialectType
}

func New(ctx context.Context, logger hclog.Logger, dsn string) (*DB, error) {
	dType, newDSN, err := ParseDialectDSN(dsn)
	if err != nil {
		return nil, err
	}

	dialect, err := schema.GetDialect(dType)
	if err != nil {
		return nil, err
	}

	db, err := postgres.NewPgDatabase(ctx, logger, newDSN, dialect)
	if err != nil {
		return nil, err
	}

	return &DB{
		Database:    db,
		dialectType: dType,
	}, nil
}

func (d *DB) DialectType() schema.DialectType {
	return d.dialectType
}
