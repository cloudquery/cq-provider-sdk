package database

import (
	"github.com/cloudquery/cq-provider-sdk/helpers"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
)

func DSNtoDialect(dsn string) (d schema.DialectType, newDSN string, err error) {
	u, err := helpers.ParseConnectionString(dsn)
	if err != nil {
		return schema.Postgres, dsn, err
	}

	switch u.Scheme {
	case "timescaledb", "tsdb", "timescale":
		u.Scheme = "postgres" // TODO remove
		return schema.TSDB, u.String(), nil
	default:
		return schema.Postgres, dsn, nil
	}
}
