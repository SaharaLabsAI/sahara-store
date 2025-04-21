package rootmulti

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"

	errorsmod "cosmossdk.io/errors"
	"cosmossdk.io/log"
	"cosmossdk.io/store/cachemulti"
	"cosmossdk.io/store/listenkv"
	"cosmossdk.io/store/mem"
	"cosmossdk.io/store/metrics"
	pruningtypes "cosmossdk.io/store/pruning/types"
	snapshottypes "cosmossdk.io/store/snapshots/types"
	"cosmossdk.io/store/tracekv"
	"cosmossdk.io/store/transient"
	"cosmossdk.io/store/types"

	db "github.com/cosmos/cosmos-db"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	protoio "github.com/cosmos/gogoproto/io"

	store "github.com/SaharaLabsAI/sahara-store"
	coretypes "github.com/SaharaLabsAI/sahara-store/core/store"
	"github.com/SaharaLabsAI/sahara-store/root"

	commstore "github.com/SaharaLabsAI/sahara-store/commitment"
	compatiavl "github.com/SaharaLabsAI/sahara-store/compatv1/iavl"
	commsnapshottypes "github.com/SaharaLabsAI/sahara-store/snapshots/types"
)

var (
	_ types.CommitMultiStore = (*Store)(nil)
	_ types.Queryable        = (*Store)(nil)
	_ io.Closer              = (*Store)(nil)
)

type Store struct {
	logger log.Logger

	root store.RootStore

	keysByName map[string]types.StoreKey
	storeTypes map[types.StoreKey]types.StoreType
	stores     map[types.StoreKey]types.CommitKVStore

	traceWriter       io.Writer
	traceContext      types.TraceContext
	traceContextMutex sync.Mutex

	listeners map[types.StoreKey]*types.MemoryListener

	metrics metrics.StoreMetrics
}

func NewStore(logger log.Logger, root store.RootStore) *Store {
	return &Store{
		logger: logger,

		root: root,

		keysByName: make(map[string]types.StoreKey),
		storeTypes: make(map[types.StoreKey]types.StoreType),
		stores:     make(map[types.StoreKey]types.CommitKVStore),

		listeners: make(map[types.StoreKey]*types.MemoryListener),
	}
}

func (s *Store) Close() error {
	return s.root.Close()
}

// AddListeners implements types.CommitMultiStore.
func (s *Store) AddListeners(keys []types.StoreKey) {
	for i := range keys {
		listener := s.listeners[keys[i]]
		if listener == nil {
			s.listeners[keys[i]] = types.NewMemoryListener()
		}
	}
}

// CacheMultiStore implements types.CommitMultiStore.
func (s *Store) CacheMultiStore() types.CacheMultiStore {
	stores := make(map[types.StoreKey]types.CacheWrapper)
	for k, v := range s.stores {
		store := types.KVStore(v)
		if s.ListeningEnabled(k) {
			store = listenkv.NewStore(store, k, s.listeners[k])
		}
		stores[k] = store
	}

	return cachemulti.NewStore(nil, stores, s.keysByName, s.traceWriter, s.getTracingContext())
}

// CacheMultiStoreWithVersion implements types.CommitMultiStore.
func (s *Store) CacheMultiStoreWithVersion(version int64) (types.CacheMultiStore, error) {
	if version == 0 {
		return s.CacheMultiStore(), nil
	}

	stores := make(map[types.StoreKey]types.CacheWrapper)
	for k, v := range s.stores {
		var cacheStore types.KVStore
		switch v.GetStoreType() {
		case types.StoreTypeIAVL:
			store, err := v.(*compatiavl.Store).GetImmutable(version)
			if err != nil {
				return nil, err
			}

			cacheStore = store
		default:
			cacheStore = v
		}

		if s.ListeningEnabled(k) {
			cacheStore = listenkv.NewStore(cacheStore, k, s.listeners[k])
		}

		stores[k] = cacheStore
	}

	return cachemulti.NewStore(nil, stores, s.keysByName, s.traceWriter, s.getTracingContext()), nil
}

// CacheWrap implements types.CommitMultiStore.
func (s *Store) CacheWrap() types.CacheWrap {
	return s.CacheMultiStore().(types.CacheWrap)
}

// CacheWrapWithTrace implements types.CommitMultiStore.
func (s *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return s.CacheWrap()
}

// Commit implements types.CommitMultiStore.
func (s *Store) Commit() types.CommitID {
	latestVersion, err := s.root.GetLatestVersion()
	if err != nil {
		panic(err)
	}

	_, err = s.root.Commit(&coretypes.Changeset{
		Version: latestVersion + 1,
		Changes: make([]coretypes.StateChanges, 0),
	})
	if err != nil {
		panic(err)
	}

	for _, store := range s.stores {
		if store.GetStoreType() != types.StoreTypeIAVL {
			_ = store.Commit()
		}
	}

	return s.LastCommitID()
}

// GetCommitKVStore implements types.CommitMultiStore.
func (s *Store) GetCommitKVStore(key types.StoreKey) types.CommitKVStore {
	return compatiavl.LoadStore(s.root, key, s.metrics)
}

// GetCommitStore implements types.CommitMultiStore.
func (s *Store) GetCommitStore(key types.StoreKey) types.CommitStore {
	return s.GetCommitKVStore(key)
}

// GetKVStore implements types.CommitMultiStore.
func (s *Store) GetKVStore(key types.StoreKey) types.KVStore {
	kvs := s.GetCommitKVStore(key)
	if s == nil {
		panic(fmt.Sprintf("store does not exist for key: %s", key.Name()))
	}

	store := types.KVStore(kvs)

	if s.TracingEnabled() {
		store = tracekv.NewStore(store, s.traceWriter, s.getTracingContext())
	}
	if s.ListeningEnabled(key) {
		store = listenkv.NewStore(store, key, s.listeners[key])
	}

	return store
}

// GetPruning implements types.CommitMultiStore.
func (s *Store) GetPruning() pruningtypes.PruningOptions {
	opt := s.root.GetPruningOption()

	switch opt.KeepRecent {
	case store.NewPruningOption(store.PruningDefault).KeepRecent:
		return pruningtypes.PruningOptions{
			KeepRecent: opt.KeepRecent,
			Interval:   opt.Interval,
			Strategy:   pruningtypes.PruningDefault,
		}
	case store.NewPruningOption(store.PruningEverything).KeepRecent:
		return pruningtypes.PruningOptions{
			KeepRecent: opt.KeepRecent,
			Interval:   opt.Interval,
			Strategy:   pruningtypes.PruningEverything,
		}
	case store.NewPruningOption(store.PruningNothing).KeepRecent:
		return pruningtypes.PruningOptions{
			KeepRecent: opt.KeepRecent,
			Interval:   opt.Interval,
			Strategy:   pruningtypes.PruningNothing,
		}
	default:
		return pruningtypes.PruningOptions{
			KeepRecent: opt.KeepRecent,
			Interval:   opt.Interval,
			Strategy:   pruningtypes.PruningCustom,
		}
	}
}

// GetStore implements types.CommitMultiStore.
func (s *Store) GetStore(key types.StoreKey) types.Store {
	return s.GetCommitKVStore(key)
}

// GetStoreType implements types.CommitMultiStore.
func (s *Store) GetStoreType() types.StoreType {
	return types.StoreTypeMulti
}

// LastCommitID implements types.CommitMultiStore.
func (s *Store) LastCommitID() types.CommitID {
	commitID, err := s.root.LastCommitID()
	if err != nil {
		panic(err)
	}

	return types.CommitID{
		Version: int64(commitID.Version),
		Hash:    commitID.Hash,
	}
}

// LatestVersion implements types.CommitMultiStore.
func (s *Store) LatestVersion() int64 {
	version, err := s.root.GetLatestVersion()
	if err != nil {
		panic(err)
	}

	return int64(version)
}

// ListeningEnabled implements types.CommitMultiStore.
func (s *Store) ListeningEnabled(key types.StoreKey) bool {
	if ls, ok := s.listeners[key]; ok {
		return ls != nil
	}
	return false
}

// LoadLatestVersion implements types.CommitMultiStore.
func (s *Store) LoadLatestVersion() error {
	latestVersion, err := s.root.GetLatestVersion()
	if err != nil {
		return err
	}

	return s.LoadVersionAndUpgrade(int64(latestVersion), nil)
}

// LoadLatestVersionAndUpgrade implements types.CommitMultiStore.
func (s *Store) LoadLatestVersionAndUpgrade(upgrades *types.StoreUpgrades) error {
	latestVersion, err := s.root.GetLatestVersion()
	if err != nil {
		return err
	}

	return s.LoadVersionAndUpgrade(int64(latestVersion), upgrades)
}

// LoadVersion implements types.CommitMultiStore.
func (s *Store) LoadVersion(ver int64) error {
	return s.LoadVersionAndUpgrade(ver, nil)
}

// LoadVersionAndUpgrade implements types.CommitMultiStore.
func (s *Store) LoadVersionAndUpgrade(ver int64, upgrades *types.StoreUpgrades) error {
	rootstore, ok := s.root.(*root.Store)
	if !ok {
		return fmt.Errorf("unexpected root store type")
	}

	var err error
	switch upgrades {
	case nil:
		err = rootstore.LoadVersion(uint64(ver))
	default:
		// FIXME: support Rename
		ups := coretypes.StoreUpgrades{
			Added:   upgrades.Added,
			Deleted: upgrades.Deleted,
		}

		err = rootstore.LoadVersionAndUpgrade(uint64(ver), &ups)
	}
	if err != nil {
		return err
	}

	newStores := make(map[types.StoreKey]types.CommitKVStore, len(s.storeTypes))
	for key, typ := range s.storeTypes {
		store, err := s.loadCommitStoreFromParams(key, typ)
		if err != nil {
			return err
		}

		newStores[key] = store
	}

	s.stores = newStores

	return nil
}

func (s *Store) MountStoreWithDB(key types.StoreKey, typ types.StoreType, _ db.DB) {
	if key == nil {
		panic("MountIAVLStore() key cannot be nil")
	}

	if _, ok := s.storeTypes[key]; ok {
		panic(fmt.Sprintf("store duplicate store key name %s", key.Name()))
	}
	if _, ok := s.keysByName[key.Name()]; ok {
		panic(fmt.Sprintf("store duplicate store key name %s", key.Name()))
	}

	s.keysByName[key.Name()] = key
	s.storeTypes[key] = typ
}

// PopStateCache implements types.CommitMultiStore.
func (s *Store) PopStateCache() []*types.StoreKVPair {
	var cache []*types.StoreKVPair
	for key := range s.listeners {
		ls := s.listeners[key]
		if ls != nil {
			cache = append(cache, ls.PopStateCache()...)
		}
	}
	sort.SliceStable(cache, func(i, j int) bool {
		return cache[i].StoreKey < cache[j].StoreKey
	})
	return cache
}

// TODO: impl prune snapshot
// PruneSnapshotHeight implements types.CommitMultiStore.
func (s *Store) PruneSnapshotHeight(height int64) {
}

func (s *Store) GetStoreByName(name string) types.Store {
	key := s.keysByName[name]
	if key == nil {
		return nil
	}

	return s.GetCommitKVStore(key)
}

// Restore implements types.CommitMultiStore.
func (s *Store) Restore(height uint64, format uint32, protoReader protoio.Reader) (snapshottypes.SnapshotItem, error) {
	var (
		importer     commstore.Importer
		snapshotItem snapshottypes.SnapshotItem
	)
loop:
	for {
		snapshotItem = snapshottypes.SnapshotItem{}
		err := protoReader.ReadMsg(&snapshotItem)
		if err == io.EOF {
			break
		} else if err != nil {
			return snapshottypes.SnapshotItem{}, errorsmod.Wrap(err, "invalid protobuf message")
		}

		switch item := snapshotItem.Item.(type) {
		case *snapshottypes.SnapshotItem_Store:
			if importer != nil {
				err = importer.Commit()
				if err != nil {
					return snapshottypes.SnapshotItem{}, errorsmod.Wrap(err, "IAVL commit failed")
				}
				importer.Close()
			}
			store, ok := s.GetStoreByName(item.Store.Name).(*compatiavl.Store)
			if !ok || store == nil {
				return snapshottypes.SnapshotItem{}, errorsmod.Wrapf(types.ErrLogic, "cannot import into non-IAVL store %q", item.Store.Name)
			}
			importer, err = store.Import(int64(height))
			if err != nil {
				return snapshottypes.SnapshotItem{}, errorsmod.Wrap(err, "import failed")
			}
			defer importer.Close()
			// Importer height must reflect the node height (which usually matches the block height, but not always)
			s.logger.Debug("restoring snapshot", "store", item.Store.Name)

		case *snapshottypes.SnapshotItem_IAVL:
			if importer == nil {
				s.logger.Error("failed to restore; received IAVL node item before store item")
				return snapshottypes.SnapshotItem{}, errorsmod.Wrap(types.ErrLogic, "received IAVL node item before store item")
			}
			if item.IAVL.Height > math.MaxInt8 {
				return snapshottypes.SnapshotItem{}, errorsmod.Wrapf(types.ErrLogic, "node height %v cannot exceed %v",
					item.IAVL.Height, math.MaxInt8)
			}
			node := &commsnapshottypes.SnapshotIAVLItem{
				Key:     item.IAVL.Key,
				Value:   item.IAVL.Value,
				Height:  item.IAVL.Height,
				Version: item.IAVL.Version,
			}
			// Protobuf does not differentiate between []byte{} as nil, but fortunately IAVL does
			// not allow nil keys nor nil values for leaf nodes, so we can always set them to empty.
			if node.Key == nil {
				node.Key = []byte{}
			}
			if node.Height == 0 && node.Value == nil {
				node.Value = []byte{}
			}
			err := importer.Add(node)
			if err != nil {
				return snapshottypes.SnapshotItem{}, errorsmod.Wrap(err, "IAVL node import failed")
			}

		default:
			break loop
		}
	}

	if importer != nil {
		err := importer.Commit()
		if err != nil {
			return snapshottypes.SnapshotItem{}, errorsmod.Wrap(err, "IAVL commit failed")
		}
		importer.Close()
	}

	_, err := s.root.Commit(&coretypes.Changeset{
		Version: height,
		Changes: make([]coretypes.StateChanges, 0),
	})
	if err != nil {
		return snapshottypes.SnapshotItem{}, err
	}

	return snapshotItem, s.LoadLatestVersion()
}

// RollbackToVersion implements types.CommitMultiStore.
func (s *Store) RollbackToVersion(target int64) error {
	if target <= 0 {
		return fmt.Errorf("invalid rollback height target: %d", target)
	}

	for key, store := range s.stores {
		if store.GetStoreType() == types.StoreTypeIAVL {
			// If the store is wrapped with an inter-block cache, we must first unwrap
			// it to get the underlying IAVL store.
			store = s.GetCommitKVStore(key)
			err := store.(*compatiavl.Store).LoadVersionForOverwriting(target)
			if err != nil {
				return err
			}
		}
	}

	_, err := s.root.Commit(&coretypes.Changeset{
		Version: uint64(target),
		Changes: make([]coretypes.StateChanges, 0),
	})

	return err
}

// SetIAVLCacheSize implements types.CommitMultiStore.
func (s *Store) SetIAVLCacheSize(_ int) {
	// Not applicable to iavl2
}

// SetIAVLDisableFastNode implements types.CommitMultiStore.
func (s *Store) SetIAVLDisableFastNode(_ bool) {
	// Not applicable to iavl2
}

// SetInitialVersion implements types.CommitMultiStore.
func (s *Store) SetInitialVersion(version int64) error {
	return s.root.SetInitialVersion(uint64(version))
}

// SetInterBlockCache implements types.CommitMultiStore.
func (s *Store) SetInterBlockCache(types.MultiStorePersistentCache) {
	// Not applicable to store v2
}

// SetMetrics implements types.CommitMultiStore.
func (s *Store) SetMetrics(metrics metrics.StoreMetrics) {
	s.metrics = metrics
}

// SetPruning implements types.CommitMultiStore.
func (s *Store) SetPruning(opt pruningtypes.PruningOptions) {
	s.root.SetPruningOption(store.PruningOption{
		KeepRecent: opt.KeepRecent,
		Interval:   opt.Interval,
	})
}

// TODO: update through snapshot manager
// SetSnapshotInterval implements types.CommitMultiStore.
func (s *Store) SetSnapshotInterval(snapshotInterval uint64) {
}

// SetTracer implements types.CommitMultiStore.
func (s *Store) SetTracer(w io.Writer) types.MultiStore {
	s.traceWriter = w
	return s
}

// SetTracingContext implements types.CommitMultiStore.
func (s *Store) SetTracingContext(tc types.TraceContext) types.MultiStore {
	s.traceContextMutex.Lock()
	defer s.traceContextMutex.Unlock()
	s.traceContext = s.traceContext.Merge(tc)

	return s
}

func (s *Store) getTracingContext() types.TraceContext {
	s.traceContextMutex.Lock()
	defer s.traceContextMutex.Unlock()

	if s.traceContext == nil {
		return nil
	}

	ctx := types.TraceContext{}
	for k, v := range s.traceContext {
		ctx[k] = v
	}

	return ctx
}

// Snapshot implements types.CommitMultiStore.
func (s *Store) Snapshot(height uint64, protoWriter protoio.Writer) error {
	if height == 0 {
		return errorsmod.Wrap(types.ErrLogic, "cannot snapshot height 0")
	}
	if height > uint64(s.LatestVersion()) {
		return errorsmod.Wrapf(types.ErrLogic, "cannot snapshot future height %v", height)
	}

	// Collect stores to snapshot (only IAVL stores are supported)
	type namedStore struct {
		*compatiavl.Store
		name string
	}

	stores := []namedStore{}
	keys := keysFromStoreKeyMap(s.stores)
	for _, key := range keys {
		switch store := s.GetCommitKVStore(key).(type) {
		case *compatiavl.Store:
			stores = append(stores, namedStore{name: key.Name(), Store: store})
		case *transient.Store, *mem.Store:
			// Non-persisted stores shouldn't be snapshotted
			continue
		default:
			return errorsmod.Wrapf(types.ErrLogic,
				"don't know how to snapshot store %q of type %T", key.Name(), store)
		}
	}
	sort.Slice(stores, func(i, j int) bool {
		return strings.Compare(stores[i].name, stores[j].name) == -1
	})

	// Export each IAVL store. Stores are serialized as a stream of SnapshotItem Protobuf
	// messages. The first item contains a SnapshotStore with store metadata (i.e. name),
	// and the following messages contain a SnapshotNode (i.e. an ExportNode). Store changes
	// are demarcated by new SnapshotStore items.
	for _, store := range stores {
		s.logger.Debug("starting snapshot", "store", store.name, "height", height)
		exporter, err := store.Export(int64(height))
		if err != nil {
			s.logger.Error("snapshot failed; exporter error", "store", store.name, "err", err)
			return err
		}

		err = func() error {
			defer exporter.Close()

			err := protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
				Item: &snapshottypes.SnapshotItem_Store{
					Store: &snapshottypes.SnapshotStoreItem{
						Name: store.name,
					},
				},
			})
			if err != nil {
				s.logger.Error("snapshot failed; item store write failed", "store", store.name, "err", err)
				return err
			}

			nodeCount := 0
			for {
				node, err := exporter.Next()
				if err == commstore.ErrorExportDone {
					s.logger.Debug("snapshot Done", "store", store.name, "nodeCount", nodeCount)
					break
				} else if err != nil {
					return err
				}
				err = protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
					Item: &snapshottypes.SnapshotItem_IAVL{
						IAVL: &snapshottypes.SnapshotIAVLItem{
							Key:     node.Key,
							Value:   node.Value,
							Height:  int32(node.Height),
							Version: node.Version,
						},
					},
				})
				if err != nil {
					return err
				}
				nodeCount++
			}

			return nil
		}()

		if err != nil {
			return err
		}
	}

	return nil
}

// TracingEnabled implements types.CommitMultiStore.
func (s *Store) TracingEnabled() bool {
	return s.traceWriter != nil
}

// WorkingHash implements types.CommitMultiStore.
func (s *Store) WorkingHash() []byte {
	storeInfos := make([]types.StoreInfo, 0, len(s.stores))

	for key, store := range s.stores {
		if store.GetStoreType() != types.StoreTypeIAVL {
			continue
		}

		si := types.StoreInfo{
			Name: key.Name(),
			CommitId: types.CommitID{
				Hash: store.WorkingHash(),
			},
		}
		storeInfos = append(storeInfos, si)
	}

	sort.SliceStable(storeInfos, func(i, j int) bool {
		return storeInfos[i].Name < storeInfos[j].Name
	})

	workingHash := types.CommitInfo{StoreInfos: storeInfos}.Hash()

	return workingHash
}

func (s *Store) Query(req *types.RequestQuery) (*types.ResponseQuery, error) {
	storeName, subpath, err := parsePath(req.Path)
	if err != nil {
		return nil, err
	}

	storeKey := s.keysByName[storeName]
	store := s.stores[storeKey]

	req.Path = subpath
	return store.(*compatiavl.Store).Query(req)
}

func (s *Store) loadCommitStoreFromParams(key types.StoreKey, typ types.StoreType) (types.CommitKVStore, error) {
	switch typ {
	case types.StoreTypeMulti:
		panic("recursive MultiStores not yet supported")
	case types.StoreTypeIAVL:
		return compatiavl.LoadStore(s.root, key, s.metrics), nil
	case types.StoreTypeDB:
		panic("recursive MultiStores not yet supported")
	case types.StoreTypeTransient:
		if _, ok := key.(*types.TransientStoreKey); !ok {
			return nil, fmt.Errorf("unexpected key type for a TransientStoreKey; got: %s, %T", key.String(), key)
		}

		return transient.NewStore(), nil
	case types.StoreTypeMemory:
		if _, ok := key.(*types.MemoryStoreKey); !ok {
			return nil, fmt.Errorf("unexpected key type for a MemoryStoreKey; got: %s", key.String())
		}

		return mem.NewStore(), nil
	default:
		panic(fmt.Sprintf("unrecognized store type %v", typ))
	}
}

func parsePath(path string) (storeName string, subpath string, err error) {
	if !strings.HasPrefix(path, "/") {
		return storeName, subpath, sdkerrors.ErrUnknownRequest.Wrapf("invalid path: %s", path)
	}

	paths := strings.SplitN(path[1:], "/", 2)
	storeName = paths[0]

	if len(paths) == 2 {
		subpath = "/" + paths[1]
	}

	return storeName, subpath, nil
}

// keysFromStoreKeyMap returns a slice of keys for the provided map lexically sorted by StoreKey.Name()
func keysFromStoreKeyMap[V any](m map[types.StoreKey]V) []types.StoreKey {
	keys := make([]types.StoreKey, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		ki, kj := keys[i], keys[j]
		return ki.Name() < kj.Name()
	})
	return keys
}
