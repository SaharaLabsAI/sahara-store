package appmodulev2

import (
	"github.com/SaharaLabsAI/sahara-store/sdk/core/branch"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/event"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/gas"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/header"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/log"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/router"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/store"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/transaction"
)

// Environment is used to get all services to their respective module.
// Contract: All fields of environment are always populated by runtime.
type Environment struct {
	Logger log.Logger

	BranchService      branch.Service
	EventService       event.Service
	GasService         gas.Service
	HeaderService      header.Service
	QueryRouterService router.Service
	MsgRouterService   router.Service
	TransactionService transaction.Service

	KVStoreService  store.KVStoreService
	MemStoreService store.MemoryStoreService
}
