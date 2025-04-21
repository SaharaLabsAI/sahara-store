package compatv1

import (
	"fmt"

	"cosmossdk.io/log"
	pruningtypes "cosmossdk.io/store/pruning/types"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/baseapp"

	store "github.com/SaharaLabsAI/sahara-store"
	"github.com/SaharaLabsAI/sahara-store/commitment/iavlv2"
	"github.com/SaharaLabsAI/sahara-store/root"

	rootmulti "github.com/SaharaLabsAI/sahara-store/compatv1/rootmulti"
)

func SetupStoreIAVL2(
	logger log.Logger,
	db dbm.DB,
	homePath string,
	baseAppOptions []func(*baseapp.BaseApp),
	storeKeyNames []string,
	appDBBackend dbm.BackendType,
	pruningOptions pruningtypes.PruningOptions,
) []func(*baseapp.BaseApp) {
	baseAppOptions = append([]func(*baseapp.BaseApp){
		setup(logger, db, homePath, storeKeyNames, appDBBackend, pruningOptions),
	}, baseAppOptions...)

	return baseAppOptions
}

func setup(
	logger log.Logger,
	db dbm.DB,
	homePath string,
	storeKeyNames []string,
	appDBBackend dbm.BackendType,
	pruningOptions pruningtypes.PruningOptions,
) func(*baseapp.BaseApp) {
	return func(bapp *baseapp.BaseApp) {
		config := &root.Config{
			Home:         homePath,
			AppDBBackend: string(appDBBackend),
			Options: root.Options{
				SCType: root.SCTypeIavlV2,
				SCPruningOption: store.NewPruningOptionWithCustom(
					pruningOptions.KeepRecent,
					pruningOptions.Interval,
				),
				IavlV2Config: iavlv2.DefaultOptions(),
			},
		}

		builder := root.NewBuilder()
		for _, key := range storeKeyNames {
			builder.RegisterKey(key)
		}

		store, err := builder.BuildWithDB(logger, db, config)
		if err != nil {
			panic(fmt.Errorf("setup store iavl v2 %s", err))
		}

		bapp.SetCMS(rootmulti.NewStore(logger, store))
	}
}
