package coretesting

import (
	"context"

	"github.com/SaharaLabsAI/sahara-store/core/gas"
)

var _ gas.Service = &TestGasService{}

type TestGasService struct{}

func (m TestGasService) GasMeter(ctx context.Context) gas.Meter {
	dummy := unwrap(ctx)

	return dummy.gasMeter
}

func (m TestGasService) GasConfig(ctx context.Context) gas.GasConfig {
	dummy := unwrap(ctx)

	return dummy.gasConfig
}
