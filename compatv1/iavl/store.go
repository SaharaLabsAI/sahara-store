package iavl

import (
	"io"

	"cosmossdk.io/store/cachekv"
	pruningtypes "cosmossdk.io/store/pruning/types"
	"cosmossdk.io/store/tracekv"

	"cosmossdk.io/store/types"

	store "github.com/SaharaLabsAI/sahara-store"
	commstore "github.com/SaharaLabsAI/sahara-store/commitment"
)

var (
	_ types.KVStore       = (*Store)(nil)
	_ types.CommitStore   = (*Store)(nil)
	_ types.CommitKVStore = (*Store)(nil)
)

type Store struct {
	storeKey types.StoreKey
	tree     commstore.Tree
}

// LoadStore from given root store, the tree version is
func LoadStore(root store.RootStore, storeKey types.StoreKey) *Store {
	tree, err := root.GetStateCommitment().(*commstore.CommitStore).GetTree(storeKey.Name())
	if err != nil {
		panic(err)
	}

	return &Store{
		storeKey: storeKey,
		tree:     tree,
	}
}

// Commit implements types.CommitStore.
func (s *Store) Commit() types.CommitID {
	panic("iavl2 store is not supposed to be commited alone")
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
	if err := s.tree.Remove(key); err != nil {
		panic(err)
	}
}

// Get implements types.KVStore.
func (s *Store) Get(key []byte) []byte {
	val, err := s.tree.GetDirty(key)
	if err != nil {
		panic(err)
	}

	return val
}

// GetStoreType implements types.KVStore.
func (s *Store) GetStoreType() types.StoreType {
	return types.StoreTypeIAVL
}

// Has implements types.KVStore.
func (s *Store) Has(key []byte) bool {
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
	if err := s.tree.Set(key, value); err != nil {
		panic(err)
	}
}

func (s *Store) GetImmutable(version uint64) (*Store, error) {
	imTree, err := s.tree.GetImmutable(version)
	if err != nil {
		return nil, err
	}

	return &Store{
		storeKey: s.storeKey,
		tree:     imTree,
	}, nil
}
