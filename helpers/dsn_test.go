package helpers

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseConnectionString(t *testing.T) {
	tbl := []struct {
		input       string
		mod         map[string]string
		expected    string
		expectError bool
	}{
		{
			input:    "postgres://a:b@c.d?x=y&z=f",
			expected: "postgres://a:b@c.d?x=y&z=f",
		},
		{
			input:    "host=localhost user=postgres password=pass database=postgres port=5432 sslmode=disable",
			expected: "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable",
		},
		{
			input:    "tsdb://a:b@c.d?x=y&z=f",
			expected: "tsdb://a:b@c.d?x=y&z=f",
		},
	}
	for _, tc := range tbl {
		out, err := ParseConnectionString(tc.input)
		if tc.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
		if err != nil {
			continue
		}

		u, err := url.Parse(tc.expected)
		assert.NoError(t, err)
		assert.EqualValues(t, u.Scheme, out.Scheme)
		assert.EqualValues(t, u.Host, out.Host)
		assert.EqualValues(t, u.Path, out.Path)
		assert.EqualValues(t, u.Query(), out.Query())
	}
}