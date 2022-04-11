package testing

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/georgysavva/scany/pgxscan"
)

func VerifyNoEmptyColumnsExcept(table string, except ...string) Verifier {
	return func(t *testing.T, conn pgxscan.Querier) {
		rows, err := conn.Query(context.Background(), fmt.Sprintf("select * into tmp from %s;", table))
		if err != nil {
			t.Fatal(err)
		}

		rows.Close()

		var dropColumns []string

		for _, column := range except {
			dropColumns = append(dropColumns, fmt.Sprintf("drop column %s", column))
		}

		rows, err = conn.Query(context.Background(), fmt.Sprintf("alter table tmp %s", strings.Join(dropColumns, ", ")))
		if err != nil {
			t.Fatal(err)
		}

		rows.Close()

		rows, err = conn.Query(context.Background(), "select t.* from tmp as t where to_jsonb(t) <> jsonb_strip_nulls(to_jsonb(t));")
		if err != nil {
			t.Fatal(err)
		}
		if rows.Next() {
			t.Fatal("VerifyNoEmptyColumnsExcept failed")
		}

		rows.Close()

		rows, err = conn.Query(context.Background(), "drop table tmp;")
		if err != nil {
			t.Fatal(err)
		}

		rows.Close()
	}
}

func VerifyOneOf(table string, oneof ...string) Verifier {
	return func(t *testing.T, conn pgxscan.Querier) {
		rows, err := conn.Query(context.Background(), fmt.Sprintf("select * from %s where num_nonnulls(%s) not in (0, 1);",
			table, strings.Join(oneof, ", ")))
		if err != nil {
			t.Fatal(err)
		}
		if rows.Next() {
			t.Fatal("VerifyOneOf failed")
		}

		rows.Close()
	}
}

func VerifyAtLeastOneRow(table string) Verifier {
	return func(t *testing.T, conn pgxscan.Querier) {
		rows, err := conn.Query(context.Background(), fmt.Sprintf("select * from %s;", table))
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			t.Fatal("VerifyAtLeastOneRow failed")
		}

		rows.Close()
	}
}
