// package clients is a wrapper around grpc clients so clients can work
// with non protobuf structs and handle unmarshaling
package clients

import (
	"context"
	"fmt"
	"io"

	"github.com/cloudquery/cq-provider-sdk/internal/pb"
	"github.com/cloudquery/cq-provider-sdk/schema"
	"github.com/cloudquery/cq-provider-sdk/spec"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc"
)

type SourceClient struct {
	pbClient pb.SourceClient
}

func NewSourceClient(cc grpc.ClientConnInterface) *SourceClient {
	return &SourceClient{
		pbClient: pb.NewSourceClient(cc),
	}
}

func (c *SourceClient) GetTables(ctx context.Context) ([]*schema.Table, error) {
	res, err := c.pbClient.GetTables(ctx, &pb.GetTables_Request{})
	if err != nil {
		return nil, err
	}
	var tables []*schema.Table
	if err := msgpack.Unmarshal(res.Tables, &tables); err != nil {
		return nil, err
	}
	return tables, nil
}

func (c *SourceClient) Fetch(ctx context.Context, spec spec.SourceSpec, res chan<- []*schema.Resource) error {
	stream, err := c.pbClient.Fetch(ctx, &pb.Fetch_Request{
		Config: []byte{},
	})
	if err != nil {
		return fmt.Errorf("failed to fetch resources: %w", err)
	}
	for {
		r, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to fetch resources from stream: %w", err)
		}
		var resources []*schema.Resource
		if err := msgpack.Unmarshal(r.Resources, &resources); err != nil {
			return fmt.Errorf("failed to unmarshal resources: %w", err)
		}
		res <- resources
	}
	return nil
}
