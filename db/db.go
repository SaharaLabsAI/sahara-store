package db

import (
	"fmt"

	coreserver "github.com/SaharaLabsAI/sahara-store/core/server"
	corestore "github.com/SaharaLabsAI/sahara-store/core/store"
)

type DBType string

const (
	DBTypeGoLevelDB DBType = "goleveldb"
	DBTypePebbleDB  DBType = "pebbledb"
	DBTypePrefixDB  DBType = "prefixdb"

	DBTypeMemDB DBType = "memdb" // used for sims

	DBFileSuffix string = ".db"
)

func NewDB(dbType DBType, name, dataDir string, opts coreserver.DynamicConfig) (corestore.KVStoreWithBatch, error) {
	switch dbType {
	case DBTypeGoLevelDB:
		return NewGoLevelDB(name, dataDir, opts)

	case DBTypePebbleDB:
		return NewPebbleDB(name, dataDir)
	case DBTypeMemDB:
		return NewMemDB(), nil
	}

	return nil, fmt.Errorf("unsupported db type: %s", dbType)
}
