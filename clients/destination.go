package clients

import "github.com/cloudquery/cq-provider-sdk/internal/pb"

type DestinationClient struct {
	pbClient pb.SourceClient
}

// func NewDestinationClient(cc grpc.ClientConnInterface) *DestinationClient {
// 	return &SourceClient{
// 		pbClient: pb.NewSourceClient(cc),
// 	}
// }
