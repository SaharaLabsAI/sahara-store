package pruning

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	iavlsql "github.com/cosmos/iavl/v2/db/sqlite"
	iavl "github.com/cosmos/iavl/v2/tree"

	sdkstore "github.com/SaharaLabsAI/sahara-store/sdk"
	"github.com/SaharaLabsAI/sahara-store/sdk/commitment"
	"github.com/SaharaLabsAI/sahara-store/sdk/commitment/iavlv2"
	corestore "github.com/SaharaLabsAI/sahara-store/sdk/core/store"
	coretesting "github.com/SaharaLabsAI/sahara-store/sdk/core/testing"
	dbm "github.com/SaharaLabsAI/sahara-store/sdk/db"
	"github.com/SaharaLabsAI/sahara-store/sdk/metrics"
)

var storeKeys = []string{"store1", "store2", "store3"}

type PruningManagerTestSuite struct {
	suite.Suite

	manager *Manager
	sc      *commitment.CommitStore
}

func TestPruningManagerTestSuite(t *testing.T) {
	suite.Run(t, &PruningManagerTestSuite{})
}

func (s *PruningManagerTestSuite) SetupTest() {
	nopLog := coretesting.NewNopLogger()
	var err error

	mdb := dbm.NewMemDB()
	multiTrees := make(map[string]commitment.CompatV1Tree)
	mountTreeFn := func(storeKey string) (commitment.CompatV1Tree, error) {
		path := fmt.Sprintf("%s/%s", s.T().TempDir(), storeKey)
		tree, err := iavlv2.NewTree(iavl.DefaultOptions(), iavlsql.Options{Path: path}, coretesting.NewNopLogger())
		require.NoError(s.T(), err)
		return tree, nil
	}
	for _, storeKey := range storeKeys {
		multiTrees[storeKey], _ = mountTreeFn(storeKey)
	}
	s.sc, err = commitment.NewCommitStore(multiTrees, nil, mdb, nopLog, metrics.NewNoOpMetrics())
	s.Require().NoError(err)

	scPruningOption := sdkstore.NewPruningOptionWithCustom(0, 1) // prune all
	s.manager = NewManager(s.sc, scPruningOption)
}

func (s *PruningManagerTestSuite) TestPrune() {
	// commit changesets with pruning
	toVersion := uint64(100)
	keyCount := 10
	for version := uint64(1); version <= toVersion; version++ {
		cs := corestore.NewChangeset(version)
		for _, storeKey := range storeKeys {
			for i := 0; i < keyCount; i++ {
				cs.Add([]byte(storeKey), []byte(fmt.Sprintf("key-%d-%d", version, i)), []byte(fmt.Sprintf("value-%d-%d", version, i)), false)
			}
		}

		s.Require().NoError(s.sc.WriteChangeset(cs))
		_, err := s.sc.Commit(version)
		s.Require().NoError(err)
		s.Require().NoError(s.manager.Prune(version))
	}

	// wait for the pruning to finish in the commitment store
	checkSCPrune := func() bool {
		count := 0
		for _, storeKey := range storeKeys {
			_, err := s.sc.GetProof([]byte(storeKey), toVersion-1, []byte(fmt.Sprintf("key-%d-%d", toVersion-1, 0)))
			if err != nil {
				count++
			}
		}

		return count == len(storeKeys)
	}
	s.Require().Eventually(checkSCPrune, 10*time.Second, 1*time.Second)
}

func TestPruningOption(t *testing.T) {
	testCases := []struct {
		name         string
		options      *sdkstore.PruningOption
		version      uint64
		pruning      bool
		pruneVersion uint64
	}{
		{
			name:         "no pruning",
			options:      sdkstore.NewPruningOptionWithCustom(100, 0),
			version:      100,
			pruning:      false,
			pruneVersion: 0,
		},
		{
			name:         "prune all",
			options:      sdkstore.NewPruningOptionWithCustom(0, 1),
			version:      19,
			pruning:      true,
			pruneVersion: 18,
		},
		{
			name:         "prune none",
			options:      sdkstore.NewPruningOptionWithCustom(100, 10),
			version:      19,
			pruning:      false,
			pruneVersion: 0,
		},
		{
			name:         "prune some",
			options:      sdkstore.NewPruningOptionWithCustom(10, 50),
			version:      100,
			pruning:      true,
			pruneVersion: 89,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pruning, pruneVersion := tc.options.ShouldPrune(tc.version)
			require.Equal(t, tc.pruning, pruning)
			require.Equal(t, tc.pruneVersion, pruneVersion)
		})
	}
}

func (s *PruningManagerTestSuite) TestSignalCommit() {
	// commit version 1
	cs := corestore.NewChangeset(1)
	for _, storeKey := range storeKeys {
		cs.Add([]byte(storeKey), []byte(fmt.Sprintf("key-%d-%d", 1, 0)), []byte(fmt.Sprintf("value-%d-%d", 1, 0)), false)
	}

	s.Require().NoError(s.sc.WriteChangeset(cs))
	_, err := s.sc.Commit(1)
	s.Require().NoError(err)

	// commit version 2
	for _, storeKey := range storeKeys {
		cs.Add([]byte(storeKey), []byte(fmt.Sprintf("key-%d-%d", 2, 0)), []byte(fmt.Sprintf("value-%d-%d", 2, 0)), false)
	}
	cs.Version = 2

	// signaling commit has started
	s.manager.PausePruning()

	s.Require().NoError(s.sc.WriteChangeset(cs))
	_, err = s.sc.Commit(2)
	s.Require().NoError(err)

	// try prune before signaling commit has finished
	s.Require().NoError(s.manager.Prune(2))

	// proof is removed no matter SignalCommit has not yet inform that commit process has finish
	// since commitInfo is remove async with tree data
	// TODO: we don't prune commit info anymore, only state
	// checkSCPrune := func() bool {
	// 	count := 0
	// 	for _, storeKey := range storeKeys {
	// 		_, err := s.sc.GetProof([]byte(storeKey), 1, []byte(fmt.Sprintf("key-%d-%d", 1, 0)))
	// 		if err != nil {
	// 			count++
	// 		}
	// 	}
	//
	// 	return count == len(storeKeys)
	// }
	// s.Require().Eventually(checkSCPrune, 10*time.Second, 1*time.Second)

	// data from state commitment should not be pruned since we haven't signal the commit process has finished
	val, err := s.sc.Get([]byte(storeKeys[0]), 1, []byte(fmt.Sprintf("key-%d-%d", 1, 0)))
	s.Require().NoError(err)
	s.Require().Equal(val, []byte(fmt.Sprintf("value-%d-%d", 1, 0)))

	// signaling commit has finished, version 1 should be pruned
	err = s.manager.ResumePruning(2)
	s.Require().NoError(err)

	checkSCPrune := func() bool {
		count := 0
		for _, storeKey := range storeKeys {
			_, err := s.sc.GetProof([]byte(storeKey), 1, []byte(fmt.Sprintf("key-%d-%d", 1, 0)))
			if err != nil {
				count++
			}
		}

		return count == len(storeKeys)
	}
	s.Require().Eventually(checkSCPrune, 10*time.Second, 1*time.Second)

	// try with signal commit start and finish accordingly
	// commit changesets with pruning
	toVersion := uint64(100)
	keyCount := 10
	for version := uint64(3); version <= toVersion; version++ {
		cs := corestore.NewChangeset(version)
		for _, storeKey := range storeKeys {
			for i := 0; i < keyCount; i++ {
				cs.Add([]byte(storeKey), []byte(fmt.Sprintf("key-%d-%d", version, i)), []byte(fmt.Sprintf("value-%d-%d", version, i)), false)
			}
		}
		s.manager.PausePruning()

		s.Require().NoError(s.sc.WriteChangeset(cs))
		_, err := s.sc.Commit(version)
		s.Require().NoError(err)

		err = s.manager.ResumePruning(version)
		s.Require().NoError(err)
	}

	// wait for the pruning to finish in the commitment store
	checkSCPrune = func() bool {
		count := 0
		for _, storeKey := range storeKeys {
			_, err := s.sc.GetProof([]byte(storeKey), toVersion-1, []byte(fmt.Sprintf("key-%d-%d", toVersion-1, 0)))
			if err != nil {
				count++
			}
		}

		return count == len(storeKeys)
	}
	s.Require().Eventually(checkSCPrune, 10*time.Second, 1*time.Second)
}
