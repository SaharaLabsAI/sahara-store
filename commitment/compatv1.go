package commitment

import corestore "github.com/SaharaLabsAI/sahara-store/core/store"

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
