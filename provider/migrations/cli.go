package migrations

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/cloudquery/cq-provider-sdk/provider"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/hashicorp/go-hclog"
	"github.com/jackc/pgx/v4/pgxpool"
)

const defaultPath = "./resources/provider/migrations"

// Run is the main entry point for CLI usage.
func Run(ctx context.Context, p *provider.Provider, outputPath string) error {
	const defaultPrefix = "unreleased_"

	if outputPath == "" {
		outputPath = defaultPath
	}

	defaultDialectsToProcess := []schema.DialectType{
		schema.Postgres,
		schema.TSDB,
	}

	outputPathParam := flag.String("path", outputPath, "Path to migrations directory")
	prefixParam := flag.String("prefix", defaultPrefix, "Prefix for files")
	doFullParam := flag.Bool("full", false, "Generate initial migrations (prefix will be 'init_')")
	dsnParam := flag.String("dsn", os.Getenv("CQ_DSN"), "DSN to compare changes against")
	dialectParam := flag.String("dialect", "", "Dialect to generate (empty: all)")
	flag.Parse()
	if flag.NArg() > 0 {
		flag.Usage()
		return fmt.Errorf("more args than necessary")
	}

	var dialects []schema.DialectType
	if *dialectParam == "" {
		dialects = defaultDialectsToProcess
	} else {
		for _, d := range defaultDialectsToProcess {
			if string(d) == *dialectParam {
				dialects = append(dialects, d)
				break
			}
		}
		if len(dialects) == 0 {
			return fmt.Errorf("invalid dialect %q", *dialectParam)
		}
	}

	if *doFullParam {
		if *prefixParam == defaultPrefix {
			*prefixParam = "init_"
		}

		if err := GenerateFull(ctx, hclog.L(), p, dialects, *outputPathParam, *prefixParam); err != nil {
			return fmt.Errorf("failed to generate migrations: %w", err)
		}
		return nil
	}

	if *dsnParam == "" {
		return fmt.Errorf("DSN not specified: Use -dsn or set CQ_DSN")
	}

	pool, err := connect(ctx, *dsnParam)
	if err != nil {
		return err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if err := GenerateDiff(ctx, hclog.L(), conn, p, dialects, *outputPathParam, *prefixParam); err != nil {
		return fmt.Errorf("failed to generate migrations: %w", err)
	}

	return nil
}

func connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	poolCfg.LazyConnect = true
	return pgxpool.ConnectConfig(ctx, poolCfg)
}
