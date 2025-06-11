package mock

import sdkstore "github.com/SaharaLabsAI/sahara-store/sdk"

// StateCommitter is a mock of store.Committer
type StateCommitter interface {
	sdkstore.Committer
	sdkstore.Pruner
	sdkstore.PausablePruner
	sdkstore.UpgradeableStore
	sdkstore.VersionedReader
	sdkstore.UpgradableDatabase
}
