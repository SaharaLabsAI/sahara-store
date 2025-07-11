module github.com/SaharaLabsAI/sahara-store/sdk

go 1.23.1

toolchain go1.23.5

replace (
	github.com/SaharaLabsAI/sahara-store/sdk/core => ./core
	github.com/SaharaLabsAI/sahara-store/sdk/core/testing => ./core/testing
	github.com/cosmos/iavl/v2 => github.com/SaharaLabsAI/iavl/v2 v2.2.0-beta.3
)

require (
	cosmossdk.io/errors/v2 v2.0.0
	cosmossdk.io/log v1.5.0
	github.com/SaharaLabsAI/sahara-store/sdk/core v0.0.0-20250611162844-38614684449c
	github.com/SaharaLabsAI/sahara-store/sdk/core/testing v0.0.0-20250611162844-38614684449c
	github.com/cockroachdb/pebble v1.1.0
	github.com/cosmos/cosmos-proto v1.0.0-beta.5
	github.com/cosmos/gogoproto v1.7.0
	github.com/cosmos/iavl/v2 v2.0.0-alpha.4
	github.com/cosmos/ics23/go v0.11.0
	github.com/google/btree v1.1.3
	github.com/hashicorp/go-metrics v0.5.4
	github.com/spf13/cast v1.7.1
	github.com/stretchr/testify v1.10.0
	github.com/syndtr/goleveldb v1.0.1-0.20220721030215-126854af5e6d
	go.uber.org/mock v0.5.0
	golang.org/x/sync v0.15.0
	google.golang.org/protobuf v1.36.6
)

require (
	cosmossdk.io/schema v1.1.0 // indirect
	github.com/DataDog/zstd v1.5.5 // indirect
	github.com/aybabtme/uniplot v0.0.0-20151203143629-039c559e5e7e // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bytedance/sonic v1.12.8 // indirect
	github.com/bytedance/sonic/loader v0.2.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.5 // indirect
	github.com/cockroachdb/errors v1.11.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/eatonphil/gosqlite v0.10.1-0.20250409163211-9c47979bc5b1 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/kocubinski/costor-api v1.1.2 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang v1.21.1 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/rs/zerolog v1.33.0 // indirect
	github.com/tidwall/btree v1.7.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	golang.org/x/arch v0.13.0 // indirect
	golang.org/x/crypto v0.39.0 // indirect
	golang.org/x/exp v0.0.0-20231006140011-7918f672742d // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	lukechampine.com/blake3 v1.4.1 // indirect
)
