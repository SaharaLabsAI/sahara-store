module github.com/SaharaLabsAI/sahara-store/sdk/core

// Core is meant to have only a dependency on cosmossdk.io/schema, so we can use it as a dependency
// in other modules without having to worry about circular dependencies.

go 1.23.0

toolchain go1.23.5

require cosmossdk.io/schema v1.0.0

// Version tagged too early and incompatible with v0.50 (latest at the time of tagging)
retract v0.12.0
