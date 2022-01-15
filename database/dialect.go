package database

import (
	"strings"

	"github.com/cloudquery/cq-provider-sdk/helpers"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
)

// ParseDialectDSN parses a DSN and returns the suggested DialectType, as well as a new version of the DSN if applicable.
// The DSN change is done to support protocol-compatible databases without needing to add support for custom URL schemes to 3rd party packages.
func ParseDialectDSN(dsn string) (d schema.DialectType, newDSN string, err error) {
	u, err := helpers.ParseConnectionString(dsn)
	if err != nil {
		return schema.Postgres, dsn, err
	}

	switch u.Scheme {
	case "timescaledb", "tsdb", "timescale":
		// TODO remove
		fixedDSN := strings.Replace(u.String(), u.Scheme+"://", "postgres://", 1)
		return schema.TSDB, fixedDSN, nil
	default:
		return schema.Postgres, dsn, nil
	}
}
