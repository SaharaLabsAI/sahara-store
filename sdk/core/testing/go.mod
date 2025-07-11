module github.com/SaharaLabsAI/sahara-store/sdk/core/testing

go 1.23.1

toolchain go1.23.4

replace github.com/SaharaLabsAI/sahara-store/sdk/core => ../../core

require (
	github.com/SaharaLabsAI/sahara-store/sdk/core v0.0.0-20250611162844-38614684449c
	github.com/cosmos/gogoproto v1.7.0
	github.com/tidwall/btree v1.7.0
	go.uber.org/mock v0.5.0
	google.golang.org/grpc v1.70.0
)

require (
	cosmossdk.io/schema v1.0.0 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250122153221-138b5a5a4fd4 // indirect
	google.golang.org/protobuf v1.36.4 // indirect
)
