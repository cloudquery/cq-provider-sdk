.PHONY: generate
generate:
	go generate ./...

.PHONY: tools
tools:
	cd tools && go install github.com/golang/protobuf/protoc-gen-go
	cd tools && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
	cd tools && go install github.com/golang/mock/mockgen