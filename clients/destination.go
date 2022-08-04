package clients

import (
	"context"
	"fmt"

	"github.com/cloudquery/cq-provider-sdk/internal/pb"
	"github.com/cloudquery/cq-provider-sdk/plugins"
	"github.com/cloudquery/cq-provider-sdk/schema"
	"github.com/cloudquery/cq-provider-sdk/spec"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

type DestinationClient struct {
	pbClient pb.DestinationClient
	// this can be used if we have a plugin which is compiled in so we dont need to do any grpc requests
	localClient plugins.DestinationPlugin
}

func NewDestinationClient(cc grpc.ClientConnInterface) *DestinationClient {
	return &DestinationClient{
		pbClient: pb.NewDestinationClient(cc),
	}
}

func NewLocalDestinationClient(p plugins.DestinationPlugin) *DestinationClient {
	return &DestinationClient{
		localClient: p,
	}
}

func (c *DestinationClient) Configure(ctx context.Context, spec spec.DestinationSpec) error {
	if c.localClient != nil {
		return c.localClient.Configure(ctx, spec)
	}
	b, err := yaml.Marshal(spec)
	if err != nil {
		return fmt.Errorf("failed to marshal spec: %w", err)
	}
	if _, err := c.pbClient.Configure(ctx, &pb.Configure_Request{DestinationSpec: b}); err != nil {
		return err
	}
	return nil
}

func (c *DestinationClient) GetExampleConfig(ctx context.Context) (string, error) {
	if c.localClient != nil {
		return c.localClient.GetExampleConfig(ctx), nil
	}
	res, err := c.pbClient.GetExampleConfig(ctx, &pb.GetExampleConfig_Request{})
	if err != nil {
		return "", err
	}
	return string(res.Config), nil
}

func (c *DestinationClient) Save(ctx context.Context, resources <-chan []byte) error {
	var saveClient pb.Destination_SaveClient
	var err error
	if c.pbClient != nil {
		saveClient, err = c.pbClient.Save(ctx)
		if err != nil {
			return fmt.Errorf("failed to create save client: %w", err)
		}
	}
	for res := range resources {
		if c.localClient != nil {
			var resources []*schema.Resource
			if err := yaml.Unmarshal(res, &resources); err != nil {
				return fmt.Errorf("failed to unmarshal resources: %w", err)
			}
			if err := c.localClient.Save(ctx, resources); err != nil {
				return fmt.Errorf("failed to save resources: %w", err)
			}
		} else {
			if err := saveClient.Send(&pb.Save_Request{Resources: res}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *DestinationClient) CreateTables(ctx context.Context, tables []*schema.Table) error {
	if c.localClient != nil {
		return c.localClient.CreateTables(ctx, tables)
	}
	b, err := yaml.Marshal(tables)
	if err != nil {
		return fmt.Errorf("failed to marshal tables: %w", err)
	}
	if _, err := c.pbClient.CreateTables(ctx, &pb.CreateTables_Request{Tables: b}); err != nil {
		return err
	}
	return nil
}
