package provider

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cloudquery/cq-provider-sdk/cqproto"
	"github.com/cloudquery/faker/v3"
	"github.com/hashicorp/go-hclog"

	"github.com/cloudquery/cq-provider-sdk/provider/schema"

	"github.com/stretchr/testify/assert"
)

type (
	testStruct struct {
		Id   int
		Name string
	}
	testConfig struct{}
)

func (t testConfig) Example() string {
	return ""
}

var (
	provider = Provider{
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
	testResolverFunc = func(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan interface{}) error {
		for i := 0; i < 10; i++ {
			t := testStruct{}
			time.Sleep(50 * time.Millisecond)
			res <- faker.FakeData(&t)
		}
		return nil
	}

	testProviderCreatorFunc = func() Provider {
		return Provider{
			Name: "unitest",
			Config: func() Config {
				return &testConfig{}
			},
			ResourceMap: map[string]*schema.Table{
				"test": {
					Name:     "test_resource",
					Resolver: testResolverFunc,
					Columns: []schema.Column{
						{
							Name: "id",
							Type: schema.TypeBigInt,
						},
						{
							Name: "name",
							Type: schema.TypeString,
						},
					},
					Relations: []*schema.Table{
						{
							Name:     "test_resource_relation",
							Resolver: testResolverFunc,
							Columns: []schema.Column{
								{
									Name: "id",
									Type: schema.TypeInt,
								},
								{
									Name: "name",
									Type: schema.TypeString,
								},
							},
						},
					},
				},
			},
		}
	}

	failProvider = Provider{
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

	parallelCheckProvider = Provider{
		Name: "paralel",
		Config: func() Config {
			return &testConfig{}
		},
		ResourceMap: map[string]*schema.Table{
			"test": {
				Name:     "test_resource",
				Resolver: testResolverFunc,
				Columns: []schema.Column{
					{
						Name: "id",
						Type: schema.TypeBigInt,
					},
					{
						Name: "name",
						Type: schema.TypeString,
					},
				},
			},
			"test1": {
				Name:     "test1_resource",
				Resolver: testResolverFunc,
				Columns: []schema.Column{
					{
						Name: "id",
						Type: schema.TypeBigInt,
					},
					{
						Name: "name",
						Type: schema.TypeString,
					},
				},
			},
			"test2": {
				Name:     "test2_resource",
				Resolver: testResolverFunc,
				Columns: []schema.Column{
					{
						Name: "id",
						Type: schema.TypeBigInt,
					},
					{
						Name: "name",
						Type: schema.TypeString,
					},
				},
			},
			"test3": {
				Name:     "test3_resource",
				Resolver: testResolverFunc,
				Columns: []schema.Column{
					{
						Name: "id",
						Type: schema.TypeBigInt,
					},
					{
						Name: "name",
						Type: schema.TypeString,
					},
				},
			},
			"test4": {
				Name:     "test4_resource",
				Resolver: testResolverFunc,
				Columns: []schema.Column{
					{
						Name: "id",
						Type: schema.TypeBigInt,
					},
					{
						Name: "name",
						Type: schema.TypeString,
					},
				},
			},
		},
	}
)

func TestProviderInterpolate(t *testing.T) {
	r, err := provider.interpolateAllResources([]string{"test"})
	assert.Nil(t, err)
	assert.ElementsMatch(t, []string{"test"}, r)

	r, err = provider.interpolateAllResources([]string{"test", "test1"})
	assert.Nil(t, err)
	assert.ElementsMatch(t, []string{"test", "test1"}, r)

	r, err = provider.interpolateAllResources([]string{"test", "test1", "*"})
	assert.Error(t, err)
	assert.Nil(t, r)
	r, err = provider.interpolateAllResources([]string{"*"})
	assert.Nil(t, err)
	assert.ElementsMatch(t, []string{"test", "test1"}, r)

}

func TestTableDuplicates(t *testing.T) {
	tables := make(map[string]string)
	var err error
	for r, t := range provider.ResourceMap {
		err = getTableDuplicates(r, t, tables)
	}
	assert.Nil(t, err)

	for r, t := range failProvider.ResourceMap {
		err = getTableDuplicates(r, t, tables)
	}
	assert.Error(t, err)
}

func TestProvider_ConfigureProvider(t *testing.T) {
	tp := testProviderCreatorFunc()
	tp.Configure = func(logger hclog.Logger, i interface{}) (schema.ClientMeta, error) {
		return nil, errors.New("test error")
	}
	resp, err := tp.ConfigureProvider(context.Background(), &cqproto.ConfigureProviderRequest{
		CloudQueryVersion: "dev",
		Connection: cqproto.ConnectionDetails{
			DSN: "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable",
		},
		Config:        nil,
		DisableDelete: true,
		ExtraFields:   nil,
	})
	assert.Equal(t, "provider unitest logger not defined, make sure to run it with serve", resp.Error)
	assert.Nil(t, err)
	// set logger this time
	tp.Logger = hclog.Default()
	resp, err = tp.ConfigureProvider(context.Background(), &cqproto.ConfigureProviderRequest{
		CloudQueryVersion: "dev",
		Connection: cqproto.ConnectionDetails{
			DSN: "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable",
		},
		Config:        nil,
		DisableDelete: true,
		ExtraFields:   nil,
	})
	assert.Equal(t, "", resp.Error)
	assert.Error(t, err)
}

type fakeResourceSender struct {
	Errors []string
}

func (f *fakeResourceSender) Send(r *cqproto.FetchResourcesResponse) error {
	if r.Error != "" {
		fmt.Printf(r.Error)
		f.Errors = append(f.Errors, r.Error)
	}
	return nil
}

type fakeClient struct {
	Log hclog.Logger
}

func (f fakeClient) Logger() hclog.Logger {
	return f.Log
}

func TestProvider_FetchResources(t *testing.T) {
	parallelCheckProvider.Configure = func(logger hclog.Logger, i interface{}) (schema.ClientMeta, error) {
		return fakeClient{Log: logger}, nil
	}
	parallelCheckProvider.Logger = hclog.Default()
	resp, err := parallelCheckProvider.ConfigureProvider(context.Background(), &cqproto.ConfigureProviderRequest{
		CloudQueryVersion: "dev",
		Connection: cqproto.ConnectionDetails{
			DSN: "postgres://postgres:pass@localhost:5432/postgres?sslmode=disable",
		},
		Config:        nil,
		DisableDelete: true,
		ExtraFields:   nil,
	})
	assert.Equal(t, "", resp.Error)
	assert.Nil(t, err)

	// it runs 5 resources at a time. each resource takes ~500ms
	start := time.Now()
	err = parallelCheckProvider.FetchResources(context.Background(), &cqproto.FetchResourcesRequest{Resources: []string{"*"}}, &fakeResourceSender{})
	assert.Nil(t, err)
	length := time.Since(start)
	assert.Less(t, length, 1000*time.Millisecond)

	// it runs 5 resources one by one. each resource takes ~500ms
	start = time.Now()
	err = parallelCheckProvider.FetchResources(context.Background(), &cqproto.FetchResourcesRequest{Resources: []string{"*"}, ParallelFetchingLimit: 1}, &fakeResourceSender{})
	assert.Nil(t, err)
	length = time.Since(start)
	assert.Greater(t, length, 2500*time.Millisecond)
}
