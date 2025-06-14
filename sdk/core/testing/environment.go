package coretesting

import (
	"context"

	appmodulev2 "github.com/SaharaLabsAI/sahara-store/sdk/core/appmodule/v2"
	corecontext "github.com/SaharaLabsAI/sahara-store/sdk/core/context"
	corelog "github.com/SaharaLabsAI/sahara-store/sdk/core/log"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/router"
	"github.com/SaharaLabsAI/sahara-store/sdk/core/store"
)

type TestEnvironmentConfig struct {
	ModuleName  string
	Logger      corelog.Logger
	MsgRouter   router.Service
	QueryRouter router.Service
}

type TestEnvironment struct {
	appmodulev2.Environment

	testEventService  TestEventService
	testHeaderService TestHeaderService
}

func NewTestEnvironment(cfg TestEnvironmentConfig) (TestContext, TestEnvironment) {
	ctx := Context()

	testEventService := NewTestEventService(ctx, cfg.ModuleName)
	testHeaderService := TestHeaderService{}

	env := TestEnvironment{
		Environment: appmodulev2.Environment{
			Logger:             cfg.Logger,
			BranchService:      TestBranchService{},
			EventService:       testEventService,
			GasService:         TestGasService{},
			HeaderService:      testHeaderService,
			QueryRouterService: cfg.QueryRouter,
			MsgRouterService:   cfg.MsgRouter,
			TransactionService: TestTransactionService{},
			KVStoreService:     KVStoreService(ctx, cfg.ModuleName),
			MemStoreService:    nil,
		},
		testEventService:  testEventService,
		testHeaderService: testHeaderService,
	}

	// set internal context to point to environment
	ctx.Context = context.WithValue(ctx.Context, corecontext.EnvironmentContextKey, env.Environment)
	return ctx, env
}

func (env TestEnvironment) EventService() TestEventService {
	return env.testEventService
}

func (env TestEnvironment) KVStoreService() store.KVStoreService {
	return env.Environment.KVStoreService
}

func (env TestEnvironment) HeaderService() TestHeaderService {
	return env.testHeaderService
}
