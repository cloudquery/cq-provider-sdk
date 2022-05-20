//go:build tools

package main

import (
	_ "github.com/golang/mock/mockgen"
	_ "github.com/golang/protobuf/protoc-gen-go"
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
)
