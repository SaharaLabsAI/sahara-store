package compatv1

import (
	"fmt"

	"cosmossdk.io/log"
	pruningtypes "cosmossdk.io/store/pruning/types"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/baseapp"
	iavl_v2 "github.com/cosmos/iavl/v2"

	store "github.com/SaharaLabsAI/sahara-store"
	"github.com/SaharaLabsAI/sahara-store/commitment/iavlv2"
	"github.com/SaharaLabsAI/sahara-store/root"

	rootmulti "github.com/SaharaLabsAI/sahara-store/compatv1/rootmulti"
)

func DefaultIavl2Options() *iavl_v2.TreeOptions {
	opts := iavlv2.DefaultOptions()

	return &opts
}

type SetupIAVL2Context struct {
	Logger            log.Logger
	HomePath          string
	Db                dbm.DB
	AppDBBackend      dbm.BackendType
	PruningOptions    pruningtypes.PruningOptions
	IavlOptions       *iavl_v2.TreeOptions
	OptimizeDBOnStart bool
	WarmCacheOnStart  bool
}

func SetupStoreIAVL2(
	ctx SetupIAVL2Context,
	baseAppOptions []func(*baseapp.BaseApp),
	storeKeyNames []string,
) []func(*baseapp.BaseApp) {
	return append([]func(*baseapp.BaseApp){setup(ctx, storeKeyNames)}, baseAppOptions...)
}

func setup(
	ctx SetupIAVL2Context,
	storeKeyNames []string,
) func(*baseapp.BaseApp) {
	iavlOpts := *ctx.IavlOptions
	if ctx.IavlOptions == nil {
		iavlOpts = iavlv2.DefaultOptions()
	}

	return func(bapp *baseapp.BaseApp) {
		config := &root.Config{
			Home:         ctx.HomePath,
			AppDBBackend: string(ctx.AppDBBackend),
			Options: root.Options{
				SCType: root.SCTypeIavlV2,
				SCPruningOption: store.NewPruningOptionWithCustom(
					ctx.PruningOptions.KeepRecent,
					ctx.PruningOptions.Interval,
				),
				IavlV2Config: iavlOpts,
				StoreDBOptions: map[string]iavl_v2.SqliteDbOptions{
					"acc": {
						MmapSize:  0,                // ensure mmap is disable completely
						CacheSize: -4 * 1024 * 1024, // 4G
					},
					"bank": {
						MmapSize:  0,                // ensure mmap is disable completely
						CacheSize: -5 * 1024 * 1024, // 5G
					},
					"distribution": {
						MmapSize:  0,                // ensure mmap is disable completely
						CacheSize: -1 * 1024 * 1024, // 1G
					},
					"evm": {
						MmapSize:  0,                // ensure mmap is disable completely
						CacheSize: -4 * 1024 * 1024, // 4G
					},
				},
				OptimizeDBOnStart: ctx.OptimizeDBOnStart,
			},
		}

		builder := root.NewBuilder()
		for _, key := range storeKeyNames {
			builder.RegisterKey(key)
		}

		store, err := builder.BuildWithDB(ctx.Logger, ctx.Db, config)
		if err != nil {
			panic(fmt.Errorf("setup store iavl v2 %s", err))
		}

		cms := rootmulti.NewStore(ctx.Logger, store)
		if ctx.WarmCacheOnStart {
			cms.SetWarmCacheOnStart()
		}

		bapp.SetCMS(cms)
	}
}
