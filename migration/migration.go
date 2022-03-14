package migration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cloudquery/cq-provider-sdk/provider"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/hashicorp/go-hclog"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/afero"
)

// GenerateFull creates initial table migrations for the provider based on it's ResourceMap
func GenerateFull(ctx context.Context, logger hclog.Logger, p *provider.Provider, dialects []schema.DialectType, outputPath, prefix string) error {
	for _, d := range dialects {
		dialect, err := schema.GetDialect(d)
		if err != nil {
			return err
		}
		if err := generateFullForDialect(ctx, logger, dialect, p, filepath.Join(outputPath, string(d)), prefix); err != nil {
			return fmt.Errorf("failed for %v: %w", d, err)
		}
	}
	return nil
}

// GenerateDiff creates incremental table migrations for the provider based on it's ResourceMap. Entities are compared to a given conn.
func GenerateDiff(ctx context.Context, logger hclog.Logger, conn *pgxpool.Conn, schemaName string, dialectType schema.DialectType, p *provider.Provider, outputPath, prefix string, fakeTSDB bool) error {
	dialect, err := schema.GetDialect(dialectType)
	if err != nil {
		return err
	}

	err = generateDiffForDialect(ctx, logger, afero.Afero{Fs: afero.OsFs{}}, conn, schemaName, dialect, p, filepath.Join(outputPath, dialectType.MigrationDirectory()), prefix, fakeTSDB)
	if err == errNoChange {
		return nil
	}
	return err
}

func generateFullForDialect(ctx context.Context, logger hclog.Logger, dialect schema.Dialect, p *provider.Provider, outputPath, prefix string) (retErr error) {
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return err
	}

	cName, dName := filepath.Join(outputPath, prefix+"up.sql"), filepath.Join(outputPath, prefix+"down.sql")

	defer func() {
		if retErr != nil {
			_ = os.Remove(cName)
			_ = os.Remove(dName)
			return
		}

		logger.Info("Generated up migrations", "filename", cName)
		logger.Info("Generated down migrations", "filename", dName)
	}()

	tc := NewTableCreator(logger, dialect)

	safeClose := func(f *os.File) {
		err := f.Close()
		if retErr == nil {
			retErr = err
		}
	}

	cf, err := os.Create(cName)
	if err != nil {
		return err
	}
	defer safeClose(cf)

	df, err := os.Create(dName)
	if err != nil {
		return err
	}
	defer safeClose(df)

	writeBoth := func(line string) {
		_, _ = cf.WriteString(line)
		_, _ = df.WriteString(line)
	}

	writeBoth(fmt.Sprintf("-- Autogenerated by migration tool on %s\n", time.Now().UTC().Format("2006-01-02 15:04:05")))

	for _, resName := range resourceKeys(p.ResourceMap) {
		table := p.ResourceMap[resName]

		writeBoth("\n-- Resource: " + resName + "\n")
		ups, downs, err := tc.CreateTableDefinitions(ctx, table, nil)
		if err != nil {
			return fmt.Errorf("CreateTable failed for %s: %w", table.Name, err)
		}

		for _, s := range ups {
			if _, err := cf.WriteString(s); err != nil {
				return err
			}
			_, _ = cf.Write([]byte{'\n'})
		}

		for _, s := range downs {
			if _, err := df.WriteString(s); err != nil {
				return err
			}
			_, _ = df.Write([]byte{'\n'})
		}
	}

	return nil
}

var errNoChange = fmt.Errorf("no change")

func generateDiffForDialect(ctx context.Context, logger hclog.Logger, fs afero.Afero, conn *pgxpool.Conn, schemaName string, dialect schema.Dialect, p *provider.Provider, outputPath, prefix string, fakeTSDB bool) (retErr error) {
	cName, dName := filepath.Join(outputPath, prefix+"up.sql"), filepath.Join(outputPath, prefix+"down.sql")

	defer func() {
		if retErr == nil {
			logger.Info("Generated up migrations", "filename", cName)
			logger.Info("Generated down migrations", "filename", dName)
			return
		}

		_ = fs.Remove(cName)
		_ = fs.Remove(dName)

		if retErr == errNoChange {
			logger.Info("Did not generate up migration (no change)")
			logger.Info("Did not generate down migration (no change)")
		}
	}()

	tc := NewTableCreator(logger, dialect)

	safeClose := func(f afero.File) {
		err := f.Close()
		if retErr == nil {
			retErr = err
		}
	}

	cf, err := fs.Create(cName)
	if err != nil {
		return err
	}
	defer safeClose(cf)

	df, err := fs.Create(dName)
	if err != nil {
		return err
	}
	defer safeClose(df)

	writeBoth := func(line string) {
		_, _ = cf.WriteString(line)
		_, _ = df.WriteString(line)
	}

	writeBoth(fmt.Sprintf("-- Autogenerated by migration tool on %s\n", time.Now().UTC().Format("2006-01-02 15:04:05")))
	writeBoth("-- CHANGEME: Verify or edit this file before proceeding\n")

	changed := false
	for _, resName := range resourceKeys(p.ResourceMap) {
		table := p.ResourceMap[resName]

		ups, downs, err := tc.DiffTable(ctx, conn, schemaName, table, nil, fakeTSDB)
		if err != nil {
			return fmt.Errorf("DiffTable failed for %s: %w", table.Name, err)
		}

		if len(ups)+len(downs) == 0 {
			continue
		}

		changed = true
		writeBoth("\n-- Resource: " + resName + "\n")

		for _, s := range ups {
			if _, err := cf.WriteString(s); err != nil {
				return err
			}
			_, _ = cf.Write([]byte{'\n'})
		}

		for _, s := range downs {
			if _, err := df.WriteString(s); err != nil {
				return err
			}
			_, _ = df.Write([]byte{'\n'})
		}
	}

	if !changed {
		return errNoChange
	}

	return nil
}

// resourceKeys gets the keys from the resourceMap and sorts them
func resourceKeys(res map[string]*schema.Table) []string {
	ret := make([]string, len(res))
	i := 0
	for k := range res {
		ret[i] = k
		i++
	}
	sort.Strings(ret)
	return ret
}
