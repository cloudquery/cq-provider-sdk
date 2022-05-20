# Contributing to CloudQuery SDK

:+1::tada: First off, thanks for taking the time to contribute! :tada::+1:

First, take a look at the contribution guide in [CloudQuery Core](https://github.com/cloudquery/cloudquery/blob/main/.github/CONTRIBUTING.md)

## Overview

CloudQuery Provider SDK is in charge of few main things:

* Common functionality to CloudQuery providers 
* Defining the protobuf for the gRPC [communication](https://docs.cloudquery.io/docs/developers/architecture) between CloudQuery Core and Providers

## Protocol Changes / Upgrades

1) Add/Change protogol at [./cqproto/internal/plugin.proto](./cqproto/internal/plugin.proto)
2) Run `make generate`. This will generate the auto-generated go files under [./cqproto/internal](./cqproto/internal/plugin.pb.go)
3) Create the wrappers interfaces for those functions and structs in [./cqproto/provider.go](./cqproto/provider.go)
4) Implement new server and client functions in [cqproto/grpc.go](cqproto/grpc.go)
5) Implement the functions themselves in [provider/provider.go](provider/provider.go)