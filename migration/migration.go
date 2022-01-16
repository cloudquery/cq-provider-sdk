package migration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cloudquery/cq-provider-sdk/provider"
	"github.com/cloudquery/cq-provider-sdk/provider/schema"
	"github.com/hashicorp/go-hclog"
	"github.com/jackc/pgx/v4/pgxpool"
)

// GenerateFull creates initial table migrations for the provider based on it's ResourceMap
func GenerateFull(ctx context.Context, logger hclog.Logger, p *provider.Provider, dialects []schema.DialectType, outputPath, prefix string) error {
	for _, d := range dialects {
		dialect, err := schema.GetDialect(d)
		if err != nil {
			return err
		}
		if err := generateFullForDialect(ctx, logger, p, dialect, filepath.Join(outputPath, string(d)), prefix); err != nil {
			return fmt.Errorf("failed for %v: %w", d, err)
		}
	}
	return nil
}

// GenerateDiff creates incremental table migrations for the provider based on it's ResourceMap. Entities are compared to a given conn.
func GenerateDiff(ctx context.Context, logger hclog.Logger, conn *pgxpool.Conn, dialectType schema.DialectType, p *provider.Provider, outputPath, prefix string) error {
	dialect, err := schema.GetDialect(dialectType)
	if err != nil {
		return err
	}
	return generateDiffForDialect(ctx, logger, conn, p, dialect, filepath.Join(outputPath, dialectType.MigrationDirectory()), prefix)
}

func generateFullForDialect(ctx context.Context, logger hclog.Logger, p *provider.Provider, dialect schema.Dialect, outputPath, prefix string) (retErr error) {
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
			_, _ = cf.Write([]byte{';', '\n'})
		}

		for _, s := range downs {
			if _, err := df.WriteString(s); err != nil {
				return err
			}
			_, _ = df.Write([]byte{';', '\n'})
		}
	}

	return nil
}

func generateDiffForDialect(ctx context.Context, logger hclog.Logger, conn *pgxpool.Conn, p *provider.Provider, dialect schema.Dialect, outputPath, prefix string) (retErr error) {
	cName, dName := filepath.Join(outputPath, prefix+"up.sql"), filepath.Join(outputPath, prefix+"down.sql")

	var errNoChange = fmt.Errorf("no change")

	defer func() {
		if retErr == nil {
			logger.Info("Generated up migrations", "filename", cName)
			logger.Info("Generated down migrations", "filename", dName)
			return
		}

		_ = os.Remove(cName)
		_ = os.Remove(dName)

		if retErr == errNoChange {
			retErr = nil
			logger.Info("Did not generate up migration (no change)")
			logger.Info("Did not generate down migration (no change)")
		}
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
	writeBoth("-- CHANGEME: Verify or edit this file before proceeding\n")

	changed := false
	for _, resName := range resourceKeys(p.ResourceMap) {
		table := p.ResourceMap[resName]

		ups, downs, err := tc.DiffTable(ctx, conn, table, nil)
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
			if !strings.Contains(s, ";") {
				_, _ = cf.Write([]byte{';'})
			}
			_, _ = cf.Write([]byte{'\n'})
		}

		for _, s := range downs {
			if _, err := df.WriteString(s); err != nil {
				return err
			}
			if !strings.Contains(s, ";") {
				_, _ = df.Write([]byte{';'})
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
