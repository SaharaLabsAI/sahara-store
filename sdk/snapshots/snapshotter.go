package snapshots

import (
	protoio "github.com/cosmos/gogoproto/io"

	"github.com/SaharaLabsAI/sahara-store/sdk/snapshots/types"
)

// CommitSnapshotter defines an API for creating and restoring snapshots of the
// commitment state.
type CommitSnapshotter interface {
	// Snapshot writes a snapshot of the commitment state at the given version.
	Snapshot(version uint64, protoWriter protoio.Writer) error

	// Restore restores the commitment state from the snapshot reader.
	Restore(version uint64, format uint32, protoReader protoio.Reader) (types.SnapshotItem, error)
}

// ExtensionPayloadReader read extension payloads,
// it returns io.EOF when reached either end of stream or the extension boundaries.
type ExtensionPayloadReader = func() ([]byte, error)

// ExtensionPayloadWriter is a helper to write extension payloads to underlying stream.
type ExtensionPayloadWriter = func([]byte) error

// ExtensionSnapshotter is an extension Snapshotter that is appended to the snapshot stream.
// ExtensionSnapshotter has an unique name and manages it's own internal formats.
type ExtensionSnapshotter interface {
	// SnapshotName returns the name of snapshotter, it should be unique in the manager.
	SnapshotName() string

	// SnapshotFormat returns the default format the extension snapshotter use to encode the
	// payloads when taking a snapshot.
	// It's defined within the extension, different from the global format for the whole state-sync snapshot.
	SnapshotFormat() uint32

	// SupportedFormats returns a list of formats it can restore from.
	SupportedFormats() []uint32

	// SnapshotExtension writes extension payloads into the underlying protobuf stream.
	SnapshotExtension(height uint64, payloadWriter ExtensionPayloadWriter) error

	// RestoreExtension restores an extension state snapshot,
	// the payload reader returns `io.EOF` when reached the extension boundaries.
	RestoreExtension(height uint64, format uint32, payloadReader ExtensionPayloadReader) error
}
