package root

import (
	"testing"

	"cosmossdk.io/log"
	"github.com/stretchr/testify/require"

	"github.com/SaharaLabsAI/sahara-store/db"
)

var storeKeys = []string{"store1", "store2", "store3"}

func TestFactory(t *testing.T) {
	fop := FactoryOptions{
		Logger:    log.NewNopLogger(),
		RootDir:   t.TempDir(),
		Options:   DefaultStoreOptions(),
		StoreKeys: storeKeys,
		SCRawDB:   db.NewMemDB(),
	}

	f, err := CreateRootStore(&fop)
	require.NoError(t, err)
	require.NotNil(t, f)

	fop.Options.SCType = SCTypeIavlV2
	f, err = CreateRootStore(&fop)
	require.NoError(t, err)
	require.NotNil(t, f)
}
