package appmodule

import (
	appmodulev2 "github.com/SaharaLabsAI/sahara-store/core/appmodule/v2"
)

// Environment is used to get all services to their respective module.
// Contract: All fields of environment are always populated by runtime.
type Environment = appmodulev2.Environment
