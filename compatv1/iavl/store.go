package iavl

import (
	"fmt"
	"io"

	"cosmossdk.io/store/cachekv"
	pruningtypes "cosmossdk.io/store/pruning/types"
	"cosmossdk.io/store/tracekv"

	"cosmossdk.io/store/types"

	store "github.com/SaharaLabsAI/sahara-store"
	commstore "github.com/SaharaLabsAI/sahara-store/commitment"
	iavlv2 "github.com/SaharaLabsAI/sahara-store/commitment/iavlv2"
	coretypes "github.com/SaharaLabsAI/sahara-store/core/store"

	storetypes "github.com/SaharaLabsAI/sahara-store/compatv1/types"
)

var (
	_ types.KVStore       = (*Store)(nil)
	_ types.CommitStore   = (*Store)(nil)
	_ types.CommitKVStore = (*Store)(nil)
)

type Store struct {
	storeKey types.StoreKey
	tree     iavlv2.Tree

	root store.RootStore // TODO: remove it
}

// LoadStore from given root store, the tree version is
func LoadStore(root store.RootStore, storeKey types.StoreKey) *Store {

	return &Store{
		storeKey: storeKey,
		root:     root,

		changeset: *coretypes.NewChangeset(0),
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
	tree := s.getTree()

	return types.CommitID{
		Version: int64(tree.Version()),
		Hash:    tree.Hash(),
	}
}

// SetPruning implements types.CommitStore.
func (s *Store) SetPruning(pruningtypes.PruningOptions) {
	panic("cannot set pruning options on an initialized IAVL store")
}

// WorkingHash implements types.CommitStore.
func (s *Store) WorkingHash() []byte {
	tree := s.getTree()

	return tree.WorkingHash()
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
	s.changeset.Add(storetypes.StoreKeyToActor(s.storeKey), key, nil, true)
}

// Get implements types.KVStore.
func (s *Store) Get(key []byte) []byte {
	reader := s.getReader()

	val, err := reader.Get(key)
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
	reader := s.getReader()

	has, err := reader.Has(key)
	if err != nil {
		panic(err)
	}

	return has
}

// Iterator implements types.KVStore.
func (s *Store) Iterator(start []byte, end []byte) types.Iterator {
	reader := s.getReader()

	iter, err := reader.Iterator(start, end)
	if err != nil {
		panic(err)
	}

	return iter
}

// ReverseIterator implements types.KVStore.
func (s *Store) ReverseIterator(start []byte, end []byte) types.Iterator {
	reader := s.getReader()

	iter, err := reader.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	return iter
}

// Set implements types.KVStore.
func (s *Store) Set(key []byte, value []byte) {
	s.changeset.Add(storetypes.StoreKeyToActor(s.storeKey), key, value, false)
}

func (s *Store) PopChangeSet() coretypes.Changeset {
	cs := s.changeset
	s.changeset = *coretypes.NewChangeset(0)
	return cs
}

func (s *Store) getReader() coretypes.Reader {
	_, readMap, err := s.root.StateLatest()
	if err != nil {
		panic(err)
	}

	reader, err := readMap.GetReader(storetypes.StoreKeyToActor(s.storeKey))
	if err != nil {
		panic(err)
	}

	return reader
}

func (s *Store) getTree() *iavlv2.Tree {
	committer, ok := s.root.GetStateCommitment().(*commstore.CommitStore)
	if !ok {
		panic(fmt.Sprintf("unexpected iavl2 store %s type", s.storeKey.Name()))
	}

	reader, err := committer.GetReader(s.storeKey.Name())
	if err != nil {
		panic(err)
	}

	tree, ok := reader.(*iavlv2.Tree)
	if !ok {
		panic(fmt.Sprintf("unexpected store %s reader type", s.storeKey.Name()))
	}

	return tree
}
