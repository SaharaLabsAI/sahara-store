package rootmulti

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"

	"cosmossdk.io/log"
	snapshottypes "cosmossdk.io/store/snapshots/types"
	"cosmossdk.io/store/types"

	sdkstore "github.com/SaharaLabsAI/sahara-store/sdk"
	"github.com/SaharaLabsAI/sahara-store/sdk/commitment/iavlv2"
	"github.com/SaharaLabsAI/sahara-store/sdk/root"
	"github.com/SaharaLabsAI/sahara-store/sdk/snapshots"

	compatiavl "github.com/SaharaLabsAI/sahara-store/iavl"
)

func newMultiStoreWithGeneratedData(db dbm.DB, stores uint8, storeKeys uint64, home string) *Store {
	builder := root.NewBuilder()

	keys := []*types.KVStoreKey{}
	for i := uint8(0); i < stores; i++ {
		key := types.NewKVStoreKey(fmt.Sprintf("store%v", i))
		builder.RegisterKey(key.Name())
		keys = append(keys, key)
	}

	config := &root.Config{
		Home:         home,
		AppDBBackend: "pebbledb",
		Options: root.Options{
			SCType: root.SCTypeIavlV2,
			SCPruningOption: &sdkstore.PruningOption{
				KeepRecent: 0,
				Interval:   0,
			},
			IavlV2Config: iavlv2.DefaultOptions(),
		},
	}

	logger := log.NewNopLogger()
	root, err := builder.BuildWithDB(logger, db, config)
	if err != nil {
		panic(err)
	}

	multiStore := NewStore(logger, root, db, DefaultStoreOption())

	r := rand.New(rand.NewSource(49872768940)) // Fixed seed for deterministic tests

	for i := uint8(0); i < stores; i++ {
		key := types.NewKVStoreKey(fmt.Sprintf("store%v", i))
		multiStore.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	}

	err = multiStore.LoadLatestVersion()
	if err != nil {
		panic(err)
	}

	for _, key := range keys {
		store := multiStore.GetCommitKVStore(key).(*compatiavl.Store)
		for i := uint64(0); i < storeKeys; i++ {
			k := make([]byte, 8)
			v := make([]byte, 1024)
			binary.BigEndian.PutUint64(k, i)
			_, err := r.Read(v)
			if err != nil {
				panic(err)
			}
			store.Set(k, v)
		}
	}

	multiStore.Commit()

	err = multiStore.LoadLatestVersion()
	if err != nil {
		panic(err)
	}

	return multiStore
}

func newMultiStoreWithMixedMounts(db dbm.DB, home string) *Store {
	builder := root.NewBuilder()

	keys := []*types.KVStoreKey{types.NewKVStoreKey("iavl1"), types.NewKVStoreKey("iavl2"), types.NewKVStoreKey("iavl3")}
	for _, key := range keys {
		builder.RegisterKey(key.Name())
	}

	config := &root.Config{
		Home:         home,
		AppDBBackend: "pebbledb",
		Options: root.Options{
			SCType: root.SCTypeIavlV2,
			SCPruningOption: &sdkstore.PruningOption{
				KeepRecent: 0,
				Interval:   0,
			},
			IavlV2Config: iavlv2.DefaultOptions(),
		},
	}

	root, err := builder.BuildWithDB(log.NewNopLogger(), db, config)
	if err != nil {
		panic(err)
	}

	store := NewStore(log.NewNopLogger(), root, db, DefaultStoreOption())

	store.MountStoreWithDB(types.NewKVStoreKey("iavl1"), types.StoreTypeIAVL, nil)
	store.MountStoreWithDB(types.NewKVStoreKey("iavl2"), types.StoreTypeIAVL, nil)
	store.MountStoreWithDB(types.NewKVStoreKey("iavl3"), types.StoreTypeIAVL, nil)
	store.MountStoreWithDB(types.NewTransientStoreKey("trans1"), types.StoreTypeTransient, nil)
	if err := store.LoadLatestVersion(); err != nil {
		panic(err)
	}

	return store
}

func newMultiStoreWithMixedMountsAndBasicData(db dbm.DB, home string) *Store {
	store := newMultiStoreWithMixedMounts(db, home)
	store1 := store.GetStoreByName("iavl1").(types.CommitKVStore)
	store2 := store.GetStoreByName("iavl2").(types.CommitKVStore)
	trans1 := store.GetStoreByName("trans1").(types.KVStore)

	store1.Set([]byte("a"), []byte{1})
	store1.Set([]byte("b"), []byte{1})
	store2.Set([]byte("X"), []byte{255})
	store2.Set([]byte("A"), []byte{101})
	trans1.Set([]byte("x1"), []byte{91})
	store.Commit()

	store1.Set([]byte("b"), []byte{2})
	store1.Set([]byte("c"), []byte{3})
	store2.Set([]byte("B"), []byte{102})
	store.Commit()

	store2.Set([]byte("C"), []byte{103})
	store2.Delete([]byte("X"))
	trans1.Set([]byte("x2"), []byte{92})
	store.Commit()

	return store
}

func assertStoresEqual(t *testing.T, expect, actual types.CommitKVStore, msgAndArgs ...interface{}) {
	t.Helper()
	require.Equal(t, expect.LastCommitID(), actual.LastCommitID())
	expectIter := expect.Iterator(nil, nil)
	expectMap := map[string][]byte{}
	for ; expectIter.Valid(); expectIter.Next() {
		expectMap[string(expectIter.Key())] = expectIter.Value()
	}
	require.NoError(t, expectIter.Error())

	actualIter := actual.Iterator(nil, nil)
	actualMap := map[string][]byte{}
	for ; actualIter.Valid(); actualIter.Next() {
		actualMap[string(actualIter.Key())] = actualIter.Value()
	}
	require.NoError(t, actualIter.Error())

	require.Equal(t, expectMap, actualMap, msgAndArgs...)
}

func TestMultistoreSnapshot_Checksum(t *testing.T) {
	// Chunks from different nodes must fit together, so all nodes must produce identical chunks.
	// This checksum test makes sure that the byte stream remains identical. If the test fails
	// without having changed the data (e.g. because the Protobuf or zlib encoding changes),
	// snapshottypes.CurrentFormat must be bumped.
	store := newMultiStoreWithGeneratedData(dbm.NewMemDB(), 5, 10000, t.TempDir())
	version := uint64(store.LastCommitID().Version)

	testcases := []struct {
		format      uint32
		chunkHashes []string
	}{
		{1, []string{
			"503e5b51b657055b77e88169fadae543619368744ad15f1de0736c0a20482f24",
			"e1a0daaa738eeb43e778aefd2805e3dd720798288a410b06da4b8459c4d8f72e",
			"aa048b4ee0f484965d7b3b06822cf0772cdcaad02f3b1b9055e69f2cb365ef3c",
			"7921eaa3ed4921341e504d9308a9877986a879fe216a099c86e8db66fcba4c63",
			"a4a864e6c02c9fca5837ec80dc84f650b25276ed7e4820cf7516ced9f9901b86",
			"980925390cc50f14998ecb1e87de719ca9dd7e72f5fefbe445397bf670f36c31",
		}},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(fmt.Sprintf("Format %v", tc.format), func(t *testing.T) {
			ch := make(chan io.ReadCloser)
			go func() {
				streamWriter := snapshots.NewStreamWriter(ch)
				defer streamWriter.Close()
				require.NotNil(t, streamWriter)
				err := store.Snapshot(version, streamWriter)
				require.NoError(t, err)
			}()
			hashes := []string{}
			hasher := sha256.New()
			for chunk := range ch {
				hasher.Reset()
				_, err := io.Copy(hasher, chunk)
				require.NoError(t, err)
				hashes = append(hashes, hex.EncodeToString(hasher.Sum(nil)))
			}
			require.Equal(t, tc.chunkHashes, hashes,
				"Snapshot output for format %v has changed", tc.format)
		})
	}
}

func TestMultistoreSnapshot_Errors(t *testing.T) {
	store := newMultiStoreWithMixedMountsAndBasicData(dbm.NewMemDB(), t.TempDir())

	testcases := map[string]struct {
		height     uint64
		expectType error
	}{
		"0 height":       {0, nil},
		"unknown height": {9, nil},
	}
	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			err := store.Snapshot(tc.height, nil)
			require.Error(t, err)
			if tc.expectType != nil {
				require.True(t, errors.Is(err, tc.expectType))
			}
		})
	}
}

func TestMultistoreSnapshotRestore(t *testing.T) {
	source := newMultiStoreWithMixedMountsAndBasicData(dbm.NewMemDB(), t.TempDir())
	target := newMultiStoreWithMixedMounts(dbm.NewMemDB(), t.TempDir())
	version := uint64(source.LastCommitID().Version)
	require.EqualValues(t, 3, version)
	dummyExtensionItem := snapshottypes.SnapshotItem{
		Item: &snapshottypes.SnapshotItem_Extension{
			Extension: &snapshottypes.SnapshotExtensionMeta{
				Name:   "test",
				Format: 1,
			},
		},
	}

	chunks := make(chan io.ReadCloser, 100)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks)
		require.NotNil(t, streamWriter)
		defer streamWriter.Close()
		err := source.Snapshot(version, streamWriter)
		require.NoError(t, err)
		// write an extension metadata
		err = streamWriter.WriteMsg(&dummyExtensionItem)
		require.NoError(t, err)
	}()

	streamReader, err := snapshots.NewStreamReader(chunks)
	require.NoError(t, err)
	nextItem, err := target.Restore(version, snapshottypes.CurrentFormat, streamReader)
	require.NoError(t, err)
	require.Equal(t, *dummyExtensionItem.GetExtension(), *nextItem.GetExtension())

	require.Equal(t, source.LastCommitID(), target.LastCommitID())
	for _, key := range source.StoreKeysByName() {
		sourceStore := source.GetStoreByName(key.Name()).(types.CommitKVStore)
		targetStore := target.GetStoreByName(key.Name()).(types.CommitKVStore)
		switch sourceStore.GetStoreType() {
		case types.StoreTypeTransient:
			require.False(t, targetStore.Iterator(nil, nil).Valid(),
				"transient store %v not empty", key.Name())
		default:
			assertStoresEqual(t, sourceStore, targetStore, "store %q not equal", key.Name())
		}
	}
}

func benchmarkMultistoreSnapshot(b *testing.B, stores uint8, storeKeys uint64) {
	b.Helper()
	b.Skip("Noisy with slow setup time, please see https://github.com/cosmos/cosmos-sdk/issues/8855.")

	b.ReportAllocs()
	b.StopTimer()
	source := newMultiStoreWithGeneratedData(dbm.NewMemDB(), stores, storeKeys, b.TempDir())
	version := source.LastCommitID().Version
	require.EqualValues(b, 1, version)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		builder := root.NewBuilder()

		for _, key := range source.StoreKeysByName() {
			builder.RegisterKey(key.Name())
		}

		config := &root.Config{
			Home:         b.TempDir(),
			AppDBBackend: "pebbledb",
			Options: root.Options{
				SCType: root.SCTypeIavlV2,
				SCPruningOption: &sdkstore.PruningOption{
					KeepRecent: 0,
					Interval:   0,
				},
				IavlV2Config: iavlv2.DefaultOptions(),
			},
		}

		logger := log.NewNopLogger()
		root, err := builder.BuildWithDB(logger, dbm.NewMemDB(), config)
		if err != nil {
			panic(err)
		}

		target := NewStore(logger, root, dbm.NewMemDB(), DefaultStoreOption())
		for _, key := range source.StoreKeysByName() {
			target.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
		}

		err = target.LoadLatestVersion()
		require.NoError(b, err)
		require.EqualValues(b, 0, target.LastCommitID().Version)

		chunks := make(chan io.ReadCloser)
		go func() {
			streamWriter := snapshots.NewStreamWriter(chunks)
			require.NotNil(b, streamWriter)
			err := source.Snapshot(uint64(version), streamWriter)
			require.NoError(b, err)
		}()
		for reader := range chunks {
			_, err := io.Copy(io.Discard, reader)
			require.NoError(b, err)
			err = reader.Close()
			require.NoError(b, err)
		}
	}
}

func benchmarkMultistoreSnapshotRestore(b *testing.B, stores uint8, storeKeys uint64) {
	b.Helper()
	b.Skip("Noisy with slow setup time, please see https://github.com/cosmos/cosmos-sdk/issues/8855.")

	b.ReportAllocs()
	b.StopTimer()
	source := newMultiStoreWithGeneratedData(dbm.NewMemDB(), stores, storeKeys, b.TempDir())
	version := uint64(source.LastCommitID().Version)
	require.EqualValues(b, 1, version)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		builder := root.NewBuilder()

		for _, key := range source.StoreKeysByName() {
			builder.RegisterKey(key.Name())
		}

		config := &root.Config{
			Home:         b.TempDir(),
			AppDBBackend: "pebbledb",
			Options: root.Options{
				SCType: root.SCTypeIavlV2,
				SCPruningOption: &sdkstore.PruningOption{
					KeepRecent: 0,
					Interval:   0,
				},
				IavlV2Config: iavlv2.DefaultOptions(),
			},
		}

		logger := log.NewNopLogger()
		root, err := builder.BuildWithDB(logger, dbm.NewMemDB(), config)
		if err != nil {
			panic(err)
		}

		target := NewStore(logger, root, dbm.NewMemDB(), DefaultStoreOption())
		for _, key := range source.StoreKeysByName() {
			target.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
		}

		err = target.LoadLatestVersion()
		require.NoError(b, err)
		require.EqualValues(b, 0, target.LastCommitID().Version)

		chunks := make(chan io.ReadCloser)
		go func() {
			writer := snapshots.NewStreamWriter(chunks)
			require.NotNil(b, writer)
			err := source.Snapshot(version, writer)
			require.NoError(b, err)
		}()
		reader, err := snapshots.NewStreamReader(chunks)
		require.NoError(b, err)
		_, err = target.Restore(version, snapshottypes.CurrentFormat, reader)
		require.NoError(b, err)
		require.Equal(b, source.LastCommitID(), target.LastCommitID())
	}
}

func BenchmarkMultistoreSnapshot100K(b *testing.B) {
	benchmarkMultistoreSnapshot(b, 10, 10000)
}

func BenchmarkMultistoreSnapshot1M(b *testing.B) {
	benchmarkMultistoreSnapshot(b, 10, 100000)
}

func BenchmarkMultistoreSnapshotRestore100K(b *testing.B) {
	benchmarkMultistoreSnapshotRestore(b, 10, 10000)
}

func BenchmarkMultistoreSnapshotRestore1M(b *testing.B) {
	benchmarkMultistoreSnapshotRestore(b, 10, 100000)
}
