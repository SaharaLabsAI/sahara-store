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
	logger           log.Logger
	homePath         string
	db               dbm.DB
	appDBBackend     dbm.BackendType
	pruningOptions   pruningtypes.PruningOptions
	iavlOptions      *iavl_v2.TreeOptions
	optimizeOnStart  bool
	warmCacheOnStart bool
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
	iavlOpts := *ctx.iavlOptions
	if ctx.iavlOptions == nil {
		iavlOpts = iavlv2.DefaultOptions()
	}

	return func(bapp *baseapp.BaseApp) {
		config := &root.Config{
			Home:         ctx.homePath,
			AppDBBackend: string(ctx.appDBBackend),
			Options: root.Options{
				SCType: root.SCTypeIavlV2,
				SCPruningOption: store.NewPruningOptionWithCustom(
					ctx.pruningOptions.KeepRecent,
					ctx.pruningOptions.Interval,
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
					"evm": {
						MmapSize:  0,                // ensure mmap is disable completely
						CacheSize: -4 * 1024 * 1024, // 4G
					},
				},
				OptimizeDBOnStart: ctx.optimizeOnStart,
			},
		}

		builder := root.NewBuilder()
		for _, key := range storeKeyNames {
			builder.RegisterKey(key)
		}

		store, err := builder.BuildWithDB(ctx.logger, ctx.db, config)
		if err != nil {
			panic(fmt.Errorf("setup store iavl v2 %s", err))
		}

		cms := rootmulti.NewStore(ctx.logger, store)
		if ctx.warmCacheOnStart {
			cms.SetWarmCacheOnStart()
		}

		bapp.SetCMS(cms)
	}
}
