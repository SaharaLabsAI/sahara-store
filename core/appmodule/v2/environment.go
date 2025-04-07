package appmodulev2

import (
	"github.com/SaharaLabsAI/sahara-store/core/branch"
	"github.com/SaharaLabsAI/sahara-store/core/event"
	"github.com/SaharaLabsAI/sahara-store/core/gas"
	"github.com/SaharaLabsAI/sahara-store/core/header"
	"github.com/SaharaLabsAI/sahara-store/core/log"
	"github.com/SaharaLabsAI/sahara-store/core/router"
	"github.com/SaharaLabsAI/sahara-store/core/store"
	"github.com/SaharaLabsAI/sahara-store/core/transaction"
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
