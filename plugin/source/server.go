package source

import (
	"context"
	"fmt"

	"github.com/cloudquery/cq-provider-sdk/plugin/source/pb"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SourceServer struct {
	pb.UnimplementedSourceServer
	plugin *SourcePlugin
}

func (s *SourceServer) GetTables(context.Context, *pb.GetTables_Request) (*pb.GetTables_Response, error) {
	b, err := msgpack.Marshal(s.plugin.Tables)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tables: %w", err)
	}
	return &pb.GetTables_Response{
		Tables: b,
	}, nil
}

func (s *SourceServer) GetExampleConfig(context.Context, *pb.GetExampleConfig_Request) (*pb.GetExampleConfig_Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetExampleConfig not implemented")
}
func (s *SourceServer) Fetch(*pb.Fetch_Request, pb.Source_FetchServer) error {
	return status.Errorf(codes.Unimplemented, "method Fetch not implemented")
}
