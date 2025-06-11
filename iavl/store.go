package iavl

import (
	"fmt"
	"io"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/cometbft/cometbft/proto/tendermint/crypto"

	"cosmossdk.io/store/cachekv"
	"cosmossdk.io/store/metrics"
	pruningtypes "cosmossdk.io/store/pruning/types"
	"cosmossdk.io/store/tracekv"
	"cosmossdk.io/store/types"

	iavlsql "github.com/cosmos/iavl/v2/db/sqlite"
	iavl2 "github.com/cosmos/iavl/v2/tree"

	sdkstore "github.com/SaharaLabsAI/sahara-store/sdk"
	commstore "github.com/SaharaLabsAI/sahara-store/sdk/commitment"
	commiavl "github.com/SaharaLabsAI/sahara-store/sdk/commitment/iavlv2"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/log"
	"github.com/SaharaLabsAI/sahara-store/sdk/proof"

	"github.com/SaharaLabsAI/sahara-store/kv"
)

var (
	_ types.KVStore       = (*Store)(nil)
	_ types.CommitStore   = (*Store)(nil)
	_ types.CommitKVStore = (*Store)(nil)
	_ types.Queryable     = (*Store)(nil)
)

const lruCacheSize = 50_000
const warnLeavesSize = 100_000

type Store struct {
	tree  commstore.CompatV1Tree
	cache *lru.Cache[string, any]

	lock    sync.RWMutex
	metrics metrics.StoreMetrics
}

type StoreOption func(*Store) error

// LoadStore from given root store, the tree version is
func LoadStore(root sdkstore.RootStore, storeKey types.StoreKey, storeMetrics metrics.StoreMetrics, opts ...StoreOption) *Store {
	if storeMetrics == nil {
		storeMetrics = metrics.NewNoOpMetrics()
	}

	tree, err := root.GetStateCommitment().(*commstore.CommitStore).GetTree(storeKey.Name())
	if err != nil {
		panic(err)
	}

	cache, err := lru.New[string, any](lruCacheSize)
	if err != nil {
		panic(err)
	}

	s := Store{
		tree:    tree,
		cache:   cache,
		metrics: storeMetrics,
	}

	for _, opt := range opts {
		if err := opt(&s); err != nil {
			panic(err)
		}
	}

	return &s
}

func StoreLRUCacheSize(size int) StoreOption {
	return func(s *Store) (err error) {
		s.cache, err = lru.New[string, any](size)
		return err
	}
}

func LoadStoreWithOpts(treeOpts iavl2.Options, dbOpts iavlsql.Options, log log.Logger, version int64, storeMetrics metrics.StoreMetrics) (*Store, error) {
	if storeMetrics == nil {
		storeMetrics = metrics.NewNoOpMetrics()
	}

	tree, err := commiavl.NewTree(treeOpts, dbOpts, log)
	if err != nil {
		return nil, err
	}

	if err := tree.LoadVersion(uint64(version)); err != nil {
		return nil, err
	}

	cache, err := lru.New[string, any](lruCacheSize)
	if err != nil {
		panic(err)
	}

	return &Store{
		tree:    tree,
		cache:   cache,
		metrics: storeMetrics,
	}, nil
}

func UnsafeNewStore(tree commstore.CompatV1Tree) *Store {
	cache, err := lru.New[string, any](lruCacheSize)
	if err != nil {
		panic(err)
	}

	return &Store{
		tree:    tree,
		cache:   cache,
		metrics: metrics.NewNoOpMetrics(),
	}
}

func (s *Store) WriteChangeSet() error {
	defer s.metrics.MeasureSince("store", "iavl", "write_change_set")

	s.lock.Lock()
	defer s.lock.Unlock()

	return s.tree.WriteChangeSet()
}

// Commit implements types.CommitStore.
func (s *Store) Commit() types.CommitID {
	defer s.metrics.MeasureSince("store", "iavl", "commit")

	if err := s.tree.WriteChangeSet(); err != nil {
		panic(err)
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	hash, v, err := s.tree.Commit()
	if err != nil {
		panic(err)
	}

	return types.CommitID{
		Version: int64(v),
		Hash:    hash,
	}
}

// GetPruning implements types.CommitStore.
func (s *Store) GetPruning() pruningtypes.PruningOptions {
	panic("cannot get pruning options on an initialized IAVL store")
}

// LastCommitID implements types.CommitStore.
func (s *Store) LastCommitID() types.CommitID {
	return types.CommitID{
		Version: int64(s.tree.Version()),
		Hash:    s.tree.Hash(),
	}
}

// SetPruning implements types.CommitStore.
func (s *Store) SetPruning(pruningtypes.PruningOptions) {
	panic("cannot set pruning options on an initialized IAVL store")
}

// WorkingHash implements types.CommitStore.
func (s *Store) WorkingHash() []byte {
	defer s.metrics.MeasureSince("store", "iavl", "working hash")

	s.lock.Lock()
	defer s.lock.Unlock()

	return s.tree.WorkingHash()
}

// CacheWrap implements types.KVStore.
func (s *Store) CacheWrap() types.CacheWrap {
	return cachekv.NewStore(s)
}

// CacheWrapWithTrace implements types.KVStore.
func (s *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return cachekv.NewStore(tracekv.NewStore(s, w, tc))
}

// Delete implements types.KVStore.
func (s *Store) Delete(key []byte) {
	types.AssertValidKey(key)

	defer s.metrics.MeasureSince("store", "iavl", "delete")

	s.lock.Lock()
	defer s.lock.Unlock()

	s.cache.Remove(string(key))

	if err := s.tree.Remove(key); err != nil {
		panic(err)
	}
}

// Get implements types.KVStore.
func (s *Store) Get(key []byte) []byte {
	defer s.metrics.MeasureSince("store", "iavl", "get")

	s.lock.RLock()
	defer s.lock.RUnlock()

	valueI, ok := s.cache.Get(string(key))
	if ok {
		return valueI.([]byte)
	}

	val, err := s.tree.GetDirty(key)
	if err != nil {
		panic(err)
	}

	s.cache.Add(string(key), val)

	return val
}

// GetStoreType implements types.KVStore.
func (s *Store) GetStoreType() types.StoreType {
	return types.StoreTypeIAVL
}

// Has implements types.KVStore.
func (s *Store) Has(key []byte) bool {
	defer s.metrics.MeasureSince("store", "iavl", "has")

	s.lock.RLock()
	defer s.lock.RUnlock()

	_, ok := s.cache.Get(string(key))
	if ok {
		return true
	}

	has, err := s.tree.HasDirty(key)
	if err != nil {
		panic(err)
	}

	return has
}

// Iterator implements types.KVStore.
func (s *Store) Iterator(start []byte, end []byte) types.Iterator {
	iter, err := s.tree.IteratorDirty(start, end, true)
	if err != nil {
		panic(err)
	}

	return iter
}

// ReverseIterator implements types.KVStore.
func (s *Store) ReverseIterator(start []byte, end []byte) types.Iterator {
	iter, err := s.tree.IteratorDirty(start, end, false)
	if err != nil {
		panic(err)
	}

	return iter
}

// Set implements types.KVStore.
func (s *Store) Set(key []byte, value []byte) {
	types.AssertValidKey(key)
	types.AssertValidValue(value)

	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.tree.Set(key, value); err != nil {
		panic(err)
	}

	s.cache.Add(string(key), value)
}

func (s *Store) GetImmutable(version int64) (*Store, error) {
	imTree, err := s.tree.GetImmutable(uint64(version))
	if err != nil {
		return nil, err
	}

	cache, err := lru.New[string, any](100)
	if err != nil {
		return nil, err
	}

	return &Store{
		tree:    imTree,
		cache:   cache,
		metrics: s.metrics,
	}, nil
}

func getHeight(tree commstore.CompatV1Tree, req *types.RequestQuery) int64 {
	height := uint64(req.Height)
	if height == 0 {
		latest := tree.Version()
		if tree.VersionExists(latest - 1) {
			height = latest - 1
		} else {
			height = latest
		}
	}
	return int64(height)
}

func getProofFromTree(tree commstore.CompatV1Tree, key []byte) *crypto.ProofOps {
	iProof, err := tree.GetProof(tree.Version(), key)
	if err != nil {
		panic(fmt.Sprintf("unexpected error for proof: %s", err.Error()))
	}

	commitOp := proof.NewIAVLCommitmentOp(key, iProof)

	proofOps := &crypto.ProofOps{
		Ops: make([]crypto.ProofOp, 0),
	}
	for _, op := range []proof.CommitmentOp{commitOp} {
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

	return proofOps
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

func (s *Store) Query(req *types.RequestQuery) (res *types.ResponseQuery, err error) {
	defer s.metrics.MeasureSince("store", "iavl", "query")

	if len(req.Data) == 0 {
		return &types.ResponseQuery{}, types.ErrTxDecode.Wrap("query cannot be zero length")
	}

	res = &types.ResponseQuery{
		Height: getHeight(s.tree, req),
	}

	switch req.Path {
	case "/key": // get by key
		key := req.Data

		res.Key = key
		if !s.tree.VersionExists(uint64(res.Height)) {
			res.Log = "version does not exist"
			break
		}

		imTree, err := s.tree.GetImmutable(uint64(res.Height))
		if err != nil {
			panic(fmt.Sprintf("version exists in store but could not retrieve corresponding versioned tree in store, %s", err.Error()))
		}
		defer imTree.Close()

		value, err := imTree.GetDirty(key)
		if err != nil {
			panic(err)
		}
		res.Value = value

		if !req.Prove {
			break
		}

		res.ProofOps = getProofFromTree(imTree, key)
	case "/subspace":
		pairs := kv.Pairs{ //nolint:staticcheck // We are in store v1.
			Pairs: make([]kv.Pair, 0), //nolint:staticcheck // We are in store v1.
		}

		subspace := req.Data
		res.Key = subspace

		imTree, err := s.tree.GetImmutable(uint64(res.Height))
		if err != nil {
			panic(fmt.Sprintf("version exists in store but could not retrieve corresponding versioned tree in store, %s", err.Error()))
		}
		defer imTree.Close()

		iter, err := imTree.IteratorDirty(subspace, prefixEndBytes(subspace), true)
		if err != nil {
			panic(err)
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
		return nil, types.ErrUnknownRequest.Wrapf("unexpected query path: %v", req.Path)
	}

	return res, nil
}

func (s *Store) VersionExists(version int64) bool {
	return s.tree.VersionExists(uint64(version))
}

func (s *Store) SetInitialVersion(version int64) error {
	return s.tree.SetInitialVersion(uint64(version))
}

func (s *Store) Import(version int64) (commstore.Importer, error) {
	return s.tree.Import(uint64(version))
}

func (s *Store) LoadVersionForOverwriting(version int64) error {
	s.cache.Purge()
	return s.tree.LoadVersionForOverwriting(uint64(version))
}

func (s *Store) Export(version int64) (commstore.Exporter, error) {
	return s.tree.Export(uint64(version))
}

func (s *Store) PurgeCache() {
	s.cache.Purge()
}

func (s *Store) Warm() error {
	version := s.tree.Version()

	iter, err := s.tree.RecentUpdatedLeaves(version, warnLeavesSize)
	if err != nil {
		return nil
	}
	defer iter.Close()

	for ; iter.Valid(); iter.Next() {
		// Also warm branches
		_, err := s.tree.GetFromRoot(iter.Key())
		if err != nil {
			return err
		}

		s.cache.Add(string(iter.Key()), iter.Value())
	}

	return nil
}
