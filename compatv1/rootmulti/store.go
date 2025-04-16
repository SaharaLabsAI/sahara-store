package rootmulti

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/cometbft/cometbft/proto/tendermint/crypto"

	"cosmossdk.io/log"
	"cosmossdk.io/store/cachemulti"
	"cosmossdk.io/store/mem"
	"cosmossdk.io/store/metrics"
	pruningtypes "cosmossdk.io/store/pruning/types"
	snapshotstypes "cosmossdk.io/store/snapshots/types"
	"cosmossdk.io/store/transient"
	"cosmossdk.io/store/types"

	db "github.com/cosmos/cosmos-db"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	protoio "github.com/cosmos/gogoproto/io"

	store "github.com/SaharaLabsAI/sahara-store"
	coretypes "github.com/SaharaLabsAI/sahara-store/core/store"
	"github.com/SaharaLabsAI/sahara-store/root"

	compatiavl "github.com/SaharaLabsAI/sahara-store/compatv1/iavl"

	"github.com/SaharaLabsAI/sahara-store/compatv1/kv"
	storetypes "github.com/SaharaLabsAI/sahara-store/compatv1/types"
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
}

func NewStore(logger log.Logger, root store.RootStore) *Store {
	return &Store{
		logger: logger,

		root: root,

		keysByName: make(map[string]types.StoreKey),
		storeTypes: make(map[types.StoreKey]types.StoreType),
		stores:     make(map[types.StoreKey]types.CommitKVStore),
	}
}

func (s *Store) Close() error {
	return s.root.Close()
}

// TODO: impl listener
// AddListeners implements types.CommitMultiStore.
func (s *Store) AddListeners(keys []types.StoreKey) {
}

// CacheMultiStore implements types.CommitMultiStore.
func (s *Store) CacheMultiStore() types.CacheMultiStore {
	stores := make(map[types.StoreKey]types.CacheWrapper)
	for k, v := range s.stores {
		store := types.CacheWrapper(v)
		if _, ok := store.(types.KVStore); !ok {
			continue
		}
		if s.ListeningEnabled(k) {
			// FIXME
			// store = listenkv.NewStore(kv, k, s.li)
		}
		stores[k] = store
	}

	// TODO: need confirmation
	return cachemulti.NewStore(nil, stores, nil, nil, nil)
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
			store, err := v.(*compatiavl.Store).GetImmutable(uint64(version))
			if err != nil {
				return nil, err
			}

			cacheStore = store
			if s.ListeningEnabled(k) {
				// FIXME
				// store = listenkv.NewStore(kv, k, s.li)
			}
		default:
			cacheStore = v
		}
		stores[k] = cacheStore
	}

	return cachemulti.NewStore(nil, stores, nil, nil, nil), nil
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
	hash := s.WorkingHash()
	s.logger.Error("before write change set again", "hash", fmt.Sprintf("%X", hash))

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

	hash = s.WorkingHash()
	s.logger.Error("after commit", "hash", fmt.Sprintf("%X", hash))

	hash = s.LastCommitID().Hash
	s.logger.Error("last commit id", "hash", fmt.Sprintf("%X", hash))

	return s.LastCommitID()
}

// GetCommitKVStore implements types.CommitMultiStore.
func (s *Store) GetCommitKVStore(key types.StoreKey) types.CommitKVStore {
	return s.stores[key]
}

// GetCommitStore implements types.CommitMultiStore.
func (s *Store) GetCommitStore(key types.StoreKey) types.CommitStore {
	return s.GetCommitKVStore(key)
}

// GetKVStore implements types.CommitMultiStore.
func (s *Store) GetKVStore(key types.StoreKey) types.KVStore {
	store, ok := s.GetStore(key).(types.KVStore)
	if !ok {
		panic(fmt.Sprintf("store %s is not KVStore", key.Name()))
	}

	return store
}

// TODO: impl pruning
// GetPruning implements types.CommitMultiStore.
func (s *Store) GetPruning() pruningtypes.PruningOptions {
	return pruningtypes.NewPruningOptions(pruningtypes.PruningNothing)
}

// GetStore implements types.CommitMultiStore.
func (s *Store) GetStore(key types.StoreKey) types.Store {
	store, ok := s.stores[key]
	if !ok {
		panic(fmt.Sprintf("store does not exist for key: %s", key.Name()))
	}

	return store
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

// TODO: impl listener
// ListeningEnabled implements types.CommitMultiStore.
func (s *Store) ListeningEnabled(key types.StoreKey) bool {
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

// TODO: impl listener
// PopStateCache implements types.CommitMultiStore.
func (s *Store) PopStateCache() []*types.StoreKVPair {
	return nil
}

// TODO: impl prune snapshot
// PruneSnapshotHeight implements types.CommitMultiStore.
func (s *Store) PruneSnapshotHeight(height int64) {
}

// TODO: impl snapshot
// Restore implements types.CommitMultiStore.
func (s *Store) Restore(height uint64, format uint32, protoReader protoio.Reader) (snapshotstypes.SnapshotItem, error) {
	return snapshotstypes.SnapshotItem{}, nil
}

// TODO: impl rollback, it's used when abci listenner throw an error
// RollbackToVersion implements types.CommitMultiStore.
func (s *Store) RollbackToVersion(version int64) error {
	panic("unimplemented")
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

// TODO; impl set metrics (must replace metrics from store v1)
// SetMetrics implements types.CommitMultiStore.
func (s *Store) SetMetrics(metrics metrics.StoreMetrics) {
	// s.root.SetMetrics(metrics)
}

// TODO: Update through pruning manager
// SetPruning implements types.CommitMultiStore.
func (s *Store) SetPruning(pruningtypes.PruningOptions) {
}

// TODO: update through snapshot manager
// SetSnapshotInterval implements types.CommitMultiStore.
func (s *Store) SetSnapshotInterval(snapshotInterval uint64) {
}

// TODO: impl tracer
// SetTracer implements types.CommitMultiStore.
func (s *Store) SetTracer(w io.Writer) types.MultiStore {
	return nil
}

// TODO: impl tracer
// SetTracingContext implements types.CommitMultiStore.
func (s *Store) SetTracingContext(types.TraceContext) types.MultiStore {
	return nil
}

// TODO: impl snapshot
// Snapshot implements types.CommitMultiStore.
func (s *Store) Snapshot(height uint64, protoWriter protoio.Writer) error {
	return nil
}

// TODO: impl tracer
// TracingEnabled implements types.CommitMultiStore.
func (s *Store) TracingEnabled() bool {
	return false
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
		// s.logger.Error("store working hash", "store", key.Name(), "working hash", fmt.Sprintf("%X", si.CommitId.Hash))
		storeInfos = append(storeInfos, si)
	}

	sort.SliceStable(storeInfos, func(i, j int) bool {
		return storeInfos[i].Name < storeInfos[j].Name
	})

	workingHash := types.CommitInfo{StoreInfos: storeInfos}.Hash()

	s.logger.Error("workingHash, after write change set", "hash", fmt.Sprintf("%X", workingHash))

	return workingHash
}

func (s *Store) Query(req *types.RequestQuery) (*types.ResponseQuery, error) {
	version := req.Height
	storeName, subpath, err := parsePath(req.Path)
	if err != nil {
		return nil, err
	}

	res := &types.ResponseQuery{
		Height: version,
	}

	storeKey := s.keysByName[storeName]
	actor := storetypes.StoreKeyToActor(storeKey)

	switch subpath {
	case "/key": // get by key
		r, err := s.root.Query(actor, uint64(version), req.Data, req.Prove)
		if err != nil {
			return nil, err
		}

		res.Key = r.Key
		res.Value = r.Value

		if !req.Prove {
			break
		}

		proofOps := &crypto.ProofOps{
			Ops: make([]crypto.ProofOp, 0),
		}

		for _, op := range r.ProofOps {
			bz, err := op.Proof.Marshal()
			if err != nil {
				panic(err.Error())
			}

			proofOps.Ops = append(proofOps.Ops, crypto.ProofOp{
				Type: op.Type,
				Key:  op.Key,
				Data: bz,
			})
		}
	case "/subspace":
		pairs := kv.Pairs{ //nolint:staticcheck // We are in store v1.
			Pairs: make([]kv.Pair, 0), //nolint:staticcheck // We are in store v1.
		}

		readerMap, err := s.root.StateAt(uint64(version))
		if err != nil {
			return nil, err
		}

		reader, err := readerMap.GetReader(actor)
		if err != nil {
			return nil, err
		}

		subspace := req.Data
		res.Key = subspace

		iter, err := reader.Iterator(subspace, prefixEndBytes(subspace))
		if err != nil {
			return nil, err
		}

		for ; iter.Valid(); iter.Next() {
			pairs.Pairs = append(pairs.Pairs, kv.Pair{Key: iter.Key(), Value: iter.Value()}) //nolint:staticcheck // We are in store v1.
		}
		if err := iter.Close(); err != nil {
			panic(fmt.Errorf("failed to close iter: %w", err))
		}

		bz, err := pairs.Marshal()
		if err != nil {
			panic(fmt.Errorf("failed to marshal KV pairs: %w", err))
		}

		res.Value = bz
	default:
		return nil, sdkerrors.ErrUnknownRequest.Wrapf("unexpected query path: %v", req.Path)
	}

	return res, nil
}

func (s *Store) loadCommitStoreFromParams(key types.StoreKey, typ types.StoreType) (types.CommitKVStore, error) {
	switch typ {
	case types.StoreTypeMulti:
		panic("recursive MultiStores not yet supported")
	case types.StoreTypeIAVL:
		return compatiavl.LoadStore(s.root, key), nil
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

func prefixEndBytes(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}

	end := make([]byte, len(prefix))
	copy(end, prefix)

	for {
		if end[len(end)-1] != byte(255) {
			end[len(end)-1]++
			break
		}

		end = end[:len(end)-1]

		if len(end) == 0 {
			end = nil
			break
		}
	}

	return end
}
