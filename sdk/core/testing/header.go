package coretesting

import (
	"context"

	"github.com/SaharaLabsAI/sahara-store/core/header"
)

var _ header.Service = &TestHeaderService{}

type TestHeaderService struct{}

func (e TestHeaderService) HeaderInfo(ctx context.Context) header.Info {
	return unwrap(ctx).header
}
