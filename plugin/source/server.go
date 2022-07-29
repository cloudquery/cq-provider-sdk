package source

import (
	"context"
	"fmt"

	"github.com/cloudquery/cq-provider-sdk/plugin/source/pb"
	"github.com/vmihailenco/msgpack/v5"
)

type SourceServer struct {
	pb.UnimplementedSourceServer
	Provider *Provider
}

func (s *SourceServer) GetSchema(ctx context.Context, req *pb.GetProviderSchema) (*pb.GetProviderSchema_Response, error) {
	bytes, err := msgpack.Marshal(s.Provider.ResourceMap)
	if err != nil {
		return nil, fmt.Errorf("msgpack.Marshal failed for s.Provider.ResourceMap: %w", err)
	}
	return &pb.GetProviderSchema_Response{Tables: bytes}, nil
}
