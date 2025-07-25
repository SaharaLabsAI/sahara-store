package root

import (
	"errors"
	"fmt"
	"sync"

	"cosmossdk.io/log"

	"golang.org/x/sync/errgroup"

	iavl2sql "github.com/cosmos/iavl/v2/db/sqlite"
	iavl2 "github.com/cosmos/iavl/v2/tree"

	sdkstore "github.com/SaharaLabsAI/sahara-store/sdk"
	"github.com/SaharaLabsAI/sahara-store/sdk/commitment"
	"github.com/SaharaLabsAI/sahara-store/sdk/commitment/iavlv2"

	corestore "github.com/SaharaLabsAI/sahara-store/sdk/core/store"
	"github.com/SaharaLabsAI/sahara-store/sdk/internal"
	"github.com/SaharaLabsAI/sahara-store/sdk/metrics"
	"github.com/SaharaLabsAI/sahara-store/sdk/pruning"
)

type (
	SCType string
)

const (
	SCTypeIavl   SCType = "iavl"
	SCTypeIavlV2 SCType = "iavl2"
)

// Options are the options for creating a root store.
type Options struct {
	SCType            SCType                  `mapstructure:"sc-type" toml:"sc-type" comment:"State commitment database type. Currently we support: \"iavl\" and \"iavl2\""`
	SCPruningOption   *sdkstore.PruningOption `mapstructure:"sc-pruning-option" toml:"sc-pruning-option" comment:"Pruning options for state commitment"`
	IavlV2Config      iavl2.Options
	StoreDBOptions    map[string]iavl2sql.Options
	OptimizeDBOnStart bool
	WarmCacheOnStart  bool
}

// FactoryOptions are the options for creating a root store.
type FactoryOptions struct {
	Logger    log.Logger
	RootDir   string
	Options   Options
	StoreKeys []string
	SCRawDB   corestore.KVStoreWithBatch
	Metrics   metrics.StoreMetrics
}

// DefaultStoreOptions returns the default options for creating a root store.
func DefaultStoreOptions() Options {
	return Options{
		SCType: SCTypeIavlV2,
		SCPruningOption: &sdkstore.PruningOption{
			KeepRecent: 2,
			Interval:   100,
		},
		OptimizeDBOnStart: false,
		WarmCacheOnStart:  false,
	}
}

// CreateRootStore is a convenience function to create a root store based on the
// provided FactoryOptions. Strictly speaking app developers can create the root
// store directly by calling root.New, so this function is not
// necessary, but demonstrates the required steps and configuration to create a root store.
func CreateRootStore(opts *FactoryOptions) (sdkstore.RootStore, error) {
	var (
		sc  *commitment.CommitStore
		err error
	)
	if opts.Metrics == nil {
		opts.Metrics = metrics.NoOpMetrics{}
	}

	storeOpts := opts.Options

	metadata := commitment.NewMetadataStore(opts.SCRawDB)
	latestVersion, err := metadata.GetLatestVersion()
	if err != nil {
		return nil, err
	}
	if len(opts.StoreKeys) == 0 {
		lastCommitInfo, err := metadata.GetCommitInfo(latestVersion)
		if err != nil {
			return nil, err
		}
		if lastCommitInfo == nil {
			return nil, fmt.Errorf("tried to construct a root store with no store keys specified but no commit info found for version %d", latestVersion)
		}
		for _, si := range lastCommitInfo.StoreInfos {
			opts.StoreKeys = append(opts.StoreKeys, string(si.Name))
		}
	}
	removedStoreKeys, err := metadata.GetRemovedStoreKeys(latestVersion)
	if err != nil {
		return nil, err
	}

	newTreeFn := func(key string) (commitment.CompatV1Tree, error) {
		if internal.IsMemoryStoreKey(key) {
			return nil, errors.New("mem tree is removed")
		} else {
			switch storeOpts.SCType {
			case SCTypeIavl:
				return nil, errors.New("iavl support is removed")
			case SCTypeIavlV2:
				metrics := metrics.NoOpMetrics{}
				opts.Options.IavlV2Config.MetricsProxy = metrics
				dir := fmt.Sprintf("%s/data/iavl2/%s", opts.RootDir, key)

				dbOpts := iavl2sql.Options{Path: dir, Metrics: metrics, Logger: opts.Logger.With("module", "iavl2")}
				storeDbOpts, exists := opts.Options.StoreDBOptions[key]
				if exists {
					storeDbOpts.Path = dir
					storeDbOpts.Metrics = metrics
					dbOpts = storeDbOpts
				}

				if opts.Options.OptimizeDBOnStart {
					dbOpts.OptimizeOnStart = true
				}

				return iavlv2.NewTree(opts.Options.IavlV2Config, dbOpts, opts.Logger)
			default:
				return nil, errors.New("unsupported commitment store type")
			}
		}
	}

	eg := new(errgroup.Group)
	eg.SetLimit(sdkstore.MaxWriteParallelism)

	var lock sync.Mutex

	trees := make(map[string]commitment.CompatV1Tree, len(opts.StoreKeys))
	for _, key := range opts.StoreKeys {
		eg.Go(func() error {
			tree, err := newTreeFn(key)
			if err != nil {
				return err
			}

			lock.Lock()
			defer lock.Unlock()

			trees[key] = tree

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	oldTrees := make(map[string]commitment.CompatV1Tree, len(opts.StoreKeys))
	for _, key := range removedStoreKeys {
		tree, err := newTreeFn(string(key))
		if err != nil {
			return nil, err
		}
		oldTrees[string(key)] = tree
	}

	sc, err = commitment.NewCommitStore(trees, oldTrees, opts.SCRawDB, opts.Logger, opts.Metrics)
	if err != nil {
		return nil, err
	}

	pm := pruning.NewManager(sc, storeOpts.SCPruningOption)
	return New(opts.SCRawDB, opts.Logger, sc, pm, nil, opts.Metrics)
}
