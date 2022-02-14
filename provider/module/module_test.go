package module

import (
	"embed"
	"sort"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
)

//go:embed testdata/*
var testdata embed.FS

func TestEmbeddedReader(t *testing.T) {
	for _, tc := range []struct {
		PreferredVersions []uint32
		ExpectedVersion   uint32
		ExpectedFilenames []string
	}{
		{
			PreferredVersions: []uint32{2, 1},
			ExpectedVersion:   2,
			ExpectedFilenames: []string{"file.hcl"},
		},
		{
			PreferredVersions: []uint32{1},
			ExpectedVersion:   1,
			ExpectedFilenames: []string{"file1.hcl", "file2.hcl", "testdir/file3.hcl"},
		},
		{
			PreferredVersions: []uint32{3},
			ExpectedVersion:   0,
		},
	} {
		info, err := EmbeddedReader(testdata, "testdata")(hclog.NewNullLogger(), "testmod", tc.PreferredVersions)
		assert.NoError(t, err)
		assert.NotNil(t, info)

		assert.Equal(t, tc.ExpectedVersion, info.Version)
		assert.EqualValues(t, []uint32{1, 2}, info.SupportedVersions)

		if tc.ExpectedVersion == 0 {
			continue
		}

		assert.NotNil(t, info.Info["info"])

		var fnlist []string
		for _, f := range info.Info["info"] {
			fnlist = append(fnlist, f.Name)
		}
		sort.Strings(fnlist)
		assert.Equal(t, tc.ExpectedFilenames, fnlist)
	}
}
