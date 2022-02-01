package diag

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiagnostics_Squash(t *testing.T) {
	testCases := []struct {
		Name  string
		Value Diagnostics
		Want  []FlatDiag
	}{
		{
			Name: "simple squash no details",
			Value: Diagnostics{
				NewBaseError(errors.New("error test"), ERROR, RESOLVING, "a", "some summary", ""),
				NewBaseError(errors.New("error test"), ERROR, RESOLVING, "a", "some summary", ""),
			},
			Want: []FlatDiag{
				{
					Err:      "error test",
					Resource: "a",
					Type:     RESOLVING,
					Severity: ERROR,
					Summary:  "some summary",
					Description: Description{
						Resource: "a",
						Summary:  "some summary",
						Detail:   "Repeated[2]",
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			assert.Equal(t, tc.Want, FlattenDiags(tc.Value.Squash(), false))
		})
	}
}
