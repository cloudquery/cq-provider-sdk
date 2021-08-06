package provider

import (
	"testing"

	"github.com/cloudquery/cq-provider-sdk/provider/schema"

	"github.com/stretchr/testify/assert"
)

var (
	resoruce = Provider{
		ResourceMap: map[string]*schema.Table{
			"test": {
				Name: "sdk_test",
				Relations: []*schema.Table{
					{
						Name: "sdk_test_test1",
						Relations: []*schema.Table{
							{Name: "sdk_test_test1_test"},
						},
					},
				},
			},
			"test1": {
				Name:      "sdk_test1",
				Relations: []*schema.Table{},
			},
		},
	}

	failResoruce = Provider{
		ResourceMap: map[string]*schema.Table{
			"test": {
				Name: "sdk_test",
				Relations: []*schema.Table{
					{
						Name: "sdk_test1",
					},
				},
			},
			"test1": {
				Name: "sdk_test1",
			},
		},
	}
)

func TestTableDuplicates(t *testing.T) {
	err := resoruce.CheckDuplicates()
	assert.Nil(t, err)

	err = failResoruce.CheckDuplicates()
	assert.Error(t, err)
}
