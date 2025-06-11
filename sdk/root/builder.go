package root

import (
	"fmt"
	"path/filepath"

	"cosmossdk.io/log"

	sdkstore "github.com/SaharaLabsAI/sahara-store/sdk"
	corestore "github.com/SaharaLabsAI/sahara-store/sdk/core/store"
	"github.com/SaharaLabsAI/sahara-store/sdk/db"
	"github.com/SaharaLabsAI/sahara-store/sdk/metrics"
)

// Builder is the interface for a store/v2 RootStore builder.
// RootStores built by the Cosmos SDK typically involve a 2 phase initialization:
//  1. Namespace registration
//  2. Configuration and loading
//
// The Builder interface is used to facilitate this pattern.  Namespaces (store keys) are registered
// by calling RegisterKey before Build is called.  Build is then called with a Config
// object and a RootStore is returned.  Calls to Get may return the `RootStore` if Build
// was successful, but that's left up to the implementation.
type Builder interface {
	// Build creates a new store/v2 RootStore from the given Config.
	Build(log.Logger, *Config) (sdkstore.RootStore, error)

	BuildWithDB(logger log.Logger, scRawDb corestore.KVStoreWithBatch, config *Config) (sdkstore.RootStore, error)
	// RegisterKey registers a store key (namespace) to be used when building the RootStore.
	RegisterKey(string)
	// Get returns the Store.  Build should be called before calling Get or the result will be nil.
	Get() sdkstore.RootStore
}

var _ Builder = (*builder)(nil)

// builder is the default builder for a store/v2 RootStore satisfying the Store interface.
// Tangibly it combines store key registration and a top-level Config to create a RootStore by calling
// the CreateRootStore factory function.
type builder struct {
	// input
	storeKeys map[string]struct{}

	// output
	store sdkstore.RootStore
}

func NewBuilder() Builder {
	return &builder{storeKeys: make(map[string]struct{})}
}

// Build creates a new store/v2 RootStore.
func (sb *builder) Build(
	logger log.Logger,
	config *Config,
) (sdkstore.RootStore, error) {
	if sb.store != nil {
		return sb.store, nil
	}
	if config.Home == "" {
		return nil, fmt.Errorf("home directory is required")
	}

	if len(config.AppDBBackend) == 0 {
		return nil, fmt.Errorf("application db backend is required")
	}

	scRawDb, err := db.NewDB(
		db.DBType(config.AppDBBackend),
		"application",
		filepath.Join(config.Home, "data"),
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create SCRawDB: %w", err)
	}

	return sb.BuildWithDB(logger, scRawDb, config)
}

func (sb *builder) BuildWithDB(logger log.Logger, scRawDb corestore.KVStoreWithBatch, config *Config) (sdkstore.RootStore, error) {
	var storeKeys []string
	for key := range sb.storeKeys {
		storeKeys = append(storeKeys, key)
	}

	factoryOptions := &FactoryOptions{
		Logger:    logger.With("module", "store"),
		RootDir:   config.Home,
		Options:   config.Options,
		StoreKeys: storeKeys,
		SCRawDB:   scRawDb,
		Metrics:   &metrics.Metrics{},
	}

	rs, err := CreateRootStore(factoryOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create root store: %w", err)
	}

	sb.store = rs
	return sb.store, nil
}

func (sb *builder) Get() sdkstore.RootStore {
	return sb.store
}

func (sb *builder) RegisterKey(key string) {
	sb.storeKeys[key] = struct{}{}
}
