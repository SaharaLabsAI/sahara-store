package commitment

import (
	"fmt"

	corestore "github.com/SaharaLabsAI/sahara-store/core/store"
)

type CompatV1Tree interface {
	Tree
	SetDirty(key, value []byte) (bool, error)
	GetDirty(key []byte) ([]byte, error)
	HasDirty(key []byte) (bool, error)
	IteratorDirty(start, end []byte, ascending bool) (corestore.Iterator, error)
	WorkingHash() []byte
	GetImmutable(version uint64) (CompatV1Tree, error)
	SaveVersion() ([]byte, int64, error)
	VersionExists(uint64) bool
}

func (c *CommitStore) GetTree(storeKey string) (CompatV1Tree, error) {
	var tree CompatV1Tree

	if storeTree, ok := c.oldTrees[storeKey]; ok {
		tree = storeTree
	} else if storeTree, ok := c.multiTrees[storeKey]; ok {
		tree = storeTree
	} else {
		return nil, fmt.Errorf("store %s not found", storeKey)
	}

	return tree, nil
}

func (c *CommitStore) GetImmutableTree(storeKey string, version uint64) (Tree, error) {
	tree, err := c.GetTree(storeKey)
	if err != nil {
		return nil, err
	}

	return tree.GetImmutable(version)
}
