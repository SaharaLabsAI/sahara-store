package types

import (
	"unsafe"

	"cosmossdk.io/store/types"
)

func StoreKeyToActor(key types.StoreKey) []byte {
	return UnsafeStrToBytes(key.Name())
}

// UnsafeStrToBytes uses unsafe to convert string into byte array. Returned bytes
// must not be altered after this function is called as it will cause a segmentation fault.
func UnsafeStrToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s)) // ref https://github.com/golang/go/issues/53003#issuecomment-1140276077
}
