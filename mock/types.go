package mock

import "github.com/SaharaLabsAI/sahara-store"

// StateCommitter is a mock of store.Committer
type StateCommitter interface {
	store.Committer
	store.Pruner
	store.PausablePruner
	store.UpgradeableStore
	store.VersionedReader
	store.UpgradableDatabase
}
