package iavlv2

import (
	"fmt"

	iavl "github.com/SaharaLabsAI/iavl/v2/tree"
	ics23 "github.com/cosmos/ics23/go"

	"github.com/SaharaLabsAI/sahara-store/sdk/commitment"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/log"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/store"
)

var _ commitment.CompatV1Tree = &ImmutableTree{}

type ImmutableTree struct {
	tree *iavl.ImmutableTree
	log  log.Logger
	path string
}

func (i *ImmutableTree) Close() error {
	return i.tree.Close()
}

func (i *ImmutableTree) Commit() ([]byte, uint64, error) {
	return nil, 0, fmt.Errorf("unexpected commit on immutable tree")
}

func (i *ImmutableTree) Export(version uint64) (commitment.Exporter, error) {
	return nil, fmt.Errorf("unexpected export on immutable tree")
}

func (i *ImmutableTree) GetDirty(key []byte) ([]byte, error) {
	return i.tree.Get(key)
}

func (i *ImmutableTree) GetFromRoot(key []byte) ([]byte, error) {
	return i.tree.Get(key)
}

func (i *ImmutableTree) GetImmutable(version uint64) (commitment.CompatV1Tree, error) {
	t := &ImmutableTree{
		tree: i.tree,
		log:  i.log,
		path: i.path,
	}

	return t, nil
}

func (i *ImmutableTree) GetLatestVersion() (uint64, error) {
	return uint64(i.tree.Version()), nil
}

func (i *ImmutableTree) GetProof(version uint64, key []byte) (*ics23.CommitmentProof, error) {
	if err := i.tree.LoadVersion(int64(version)); err != nil {
		return nil, err
	}

	return i.tree.GetProof(key)
}

func (i *ImmutableTree) HasDirty(key []byte) (bool, error) {
	return i.tree.Has(key)
}

func (i *ImmutableTree) Hash() []byte {
	return i.tree.Hash()
}

func (i *ImmutableTree) Import(version uint64) (commitment.Importer, error) {
	return nil, fmt.Errorf("unexpected import on immutable tree")
}

func (i *ImmutableTree) IsConcurrentSafe() bool {
	return true
}

func (i *ImmutableTree) IteratorDirty(start []byte, end []byte, ascending bool) (store.Iterator, error) {
	return i.tree.Iterator(start, end, false)
}

func (i *ImmutableTree) LoadVersion(version uint64) error {
	return i.tree.LoadVersion(int64(version))
}

func (i *ImmutableTree) LoadVersionForOverwriting(version uint64) error {
	return fmt.Errorf("unexpected load version for overwriting on immutable tree")
}

func (i *ImmutableTree) Path() string {
	return i.path
}

func (i *ImmutableTree) Prune(version uint64) error {
	return fmt.Errorf("unexpected prune on immutable tree")
}

func (i *ImmutableTree) RecentUpdatedLeaves(version uint64, limit int) (store.Iterator, error) {
	return nil, fmt.Errorf("unexpected recent updated leaves on immutable tree")
}

func (i *ImmutableTree) Remove(key []byte) error {
	return fmt.Errorf("unexpected remove on immutable tree")
}

func (i *ImmutableTree) SaveVersion() ([]byte, int64, error) {
	return nil, 0, fmt.Errorf("unexpected save version on immutable tree")
}

func (i *ImmutableTree) Set(key []byte, value []byte) error {
	return fmt.Errorf("unexpected set on immutable tree")
}

func (i *ImmutableTree) SetDirty(key []byte, value []byte) (bool, error) {
	return false, fmt.Errorf("unexpected set dirty on immutable tree")
}

// Test require panic
func (i *ImmutableTree) SetInitialVersion(version uint64) error {
	panic("unexpected set initial version on immutable tree")
}

func (i *ImmutableTree) Version() uint64 {
	return uint64(i.tree.Version())
}

func (i *ImmutableTree) VersionExists(version uint64) bool {
	exists, err := i.tree.VersionExists(int64(version))
	if err != nil {
		panic(err)
	}

	return exists
}

func (i *ImmutableTree) WorkingHash() []byte {
	return i.tree.Hash()
}

func (i *ImmutableTree) WriteChangeSet() error {
	return fmt.Errorf("unexpected write change set on immutable tree")
}
