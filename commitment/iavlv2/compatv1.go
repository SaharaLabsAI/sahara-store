package iavlv2

import (
	"github.com/SaharaLabsAI/sahara-store/commitment"
	corestore "github.com/SaharaLabsAI/sahara-store/core/store"
)

var (
	_ commitment.CompatV1Tree = (*Tree)(nil)
)

func (t *Tree) SetDirty(key, val []byte) (bool, error) {
	return t.tree.Set(key, val)
}

func (t *Tree) GetDirty(key []byte) ([]byte, error) {
	return t.tree.Get(key)
}

func (t *Tree) GetFromRoot(key []byte) ([]byte, error) {
	return t.tree.GetFromRoot(key)
}

func (t *Tree) Path() string {
	return t.tree.Path()
}

func (t *Tree) HasDirty(key []byte) (bool, error) {
	return t.tree.Has(key)
}

func (t *Tree) IteratorDirty(start, end []byte, ascending bool) (corestore.Iterator, error) {
	if ascending {
		return t.tree.Iterator(start, end, false)
	}

	return t.tree.ReverseIterator(start, end)
}

func (t *Tree) WorkingHash() []byte {
	// return t.tree.Hash()
	return t.tree.WorkingHash()
}

func (t *Tree) GetImmutable(version uint64) (commitment.CompatV1Tree, error) {
	imTree, err := t.tree.GetImmutable(int64(version))
	if err != nil {
		return nil, err
	}

	return &ImmutableTree{
		tree: imTree,
		log:  t.log,
		path: t.path,
	}, nil
}

func (t *Tree) SaveVersion() ([]byte, int64, error) {
	return t.tree.SaveVersion()
}

func (t *Tree) VersionExists(version uint64) bool {
	exists, err := t.tree.VersionExists(int64(version))
	if err != nil {
		panic(err)
	}

	return exists
}

func (t *Tree) RecentUpdatedLeaves(version uint64, limit int) (corestore.Iterator, error) {
	return t.tree.IteratorLatestLeaves(int64(version), limit)
}
