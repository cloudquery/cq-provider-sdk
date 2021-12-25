package sqldiff

import (
	"fmt"
	"reflect"
)

type SqlDiff struct {
	ignoreColumns map[string]bool
}

type Diff struct {
	// in the future we can create smarter diffs if needed with Type/Operation (change, add, remove, etc...) + text
	// for various classification purposes
	Text string
}

// New creates a new SqlDiff
// ignoreColumns is array of columns to ignore in comparision
func New(ignoreColumns []string) *SqlDiff {
	s := SqlDiff{
		ignoreColumns: map[string]bool{
			"last_updated": true,
			"cq_id":        true,
			"meta":         true,
		},
	}
	for _, c := range ignoreColumns {
		s.ignoreColumns[c] = true
	}
	return &s
}

// CompareTwoResults compare results of two
// "select * from table" aggregated as json.
// returns array of diffs
func (s *SqlDiff) CompareTwoResults(a []map[string]interface{}, b []map[string]interface{}) []Diff {
	var diff []Diff
	if len(a) != len(b) {
		diff = append(diff, Diff{
			Text: fmt.Sprintf("number of rows is different %d, %d", len(a), len(b)),
		})
		return diff
	}

	for i, _ := range a {
		rowsDiff := s.CompareTwoRows(a[i], b[i])
		if len(rowsDiff) != 0 {
			return append(diff, rowsDiff...)
		}
	}

	return diff
}

func (s *SqlDiff) CompareTwoRows(a map[string]interface{}, b map[string]interface{}) []Diff {
	var diffs []Diff
	if len(a) != len(b) {
		// in that case we exit and we are not continuing to check others diffs
		diffs = append(diffs, Diff{
			Text: fmt.Sprintf("number of columns is different %d, %d", len(a), len(b)),
		})
		return diffs
	}

	for c := range a {
		if !s.ignoreColumns[c] && !reflect.DeepEqual(a[c], b[c]) {
			diffs = append(diffs, Diff{
				Text: fmt.Sprintf("value in column %s is different\nGot %s\nExpected %s\n", c, a[c], b[c]),
			})
		}
	}

	return diffs
}
