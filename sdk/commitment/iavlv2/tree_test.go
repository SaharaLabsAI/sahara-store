package iavlv2

import (
	"fmt"
	"testing"

	iavlsql "github.com/cosmos/iavl/v2/db/sqlite"
	iavl "github.com/cosmos/iavl/v2/tree"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/SaharaLabsAI/sahara-store/sdk/commitment"
	corelog "github.com/SaharaLabsAI/sahara-store/sdk/core/log"
	corestore "github.com/SaharaLabsAI/sahara-store/sdk/core/store"
	"github.com/SaharaLabsAI/sahara-store/sdk/metrics"
)

func TestCommitterSuite(t *testing.T) {
	s := &commitment.CommitStoreTestSuite{
		TreeType: "iavlv2",
		NewStore: func(
			db corestore.KVStoreWithBatch,
			dbDir string,
			storeKeys, oldStoreKeys []string,
			logger corelog.Logger,
		) (*commitment.CommitStore, error) {
			multiTrees := make(map[string]commitment.CompatV1Tree)
			mountTreeFn := func(storeKey string) (commitment.CompatV1Tree, error) {
				path := fmt.Sprintf("%s/%s", dbDir, storeKey)
				tree, err := NewTree(iavl.DefaultOptions(), iavlsql.Options{Path: path}, logger)
				require.NoError(t, err)
				return tree, nil
			}
			for _, storeKey := range storeKeys {
				multiTrees[storeKey], _ = mountTreeFn(storeKey)
			}
			oldTrees := make(map[string]commitment.CompatV1Tree)
			for _, storeKey := range oldStoreKeys {
				oldTrees[storeKey], _ = mountTreeFn(storeKey)
			}

			return commitment.NewCommitStore(multiTrees, oldTrees, db, logger, metrics.NewNoOpMetrics())
		},
	}

	suite.Run(t, s)
}
