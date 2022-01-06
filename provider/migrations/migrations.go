package migrations

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cloudquery/cq-provider-sdk/provider"
	"github.com/hashicorp/go-hclog"
)

// Generate creates table migrations for the provider based on it's ResourceMap
func Generate(ctx context.Context, logger hclog.Logger, p *provider.Provider, outputPath, prefix string) (retErr error) {
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

	tc := NewTableCreator(logger)

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

	for resName, table := range p.ResourceMap {
		writeBoth("\n-- Resource: " + resName + "\n")

		create, drop, err := tc.CreateTable(ctx, table, nil)
		if err != nil {
			return fmt.Errorf("CreateTable failed for %s: %w", table.Name, err)
		}

		for _, s := range create {
			if _, err := cf.WriteString(s); err != nil {
				return err
			}
			_, _ = cf.Write([]byte{'\n'})
		}

		for _, s := range drop {
			if _, err := df.WriteString(s); err != nil {
				return err
			}
			_, _ = df.Write([]byte{'\n'})
		}
	}

	return nil
}
