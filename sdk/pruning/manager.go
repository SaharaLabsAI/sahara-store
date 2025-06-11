package pruning

import (
	sdkstore "github.com/SaharaLabsAI/sahara-store/sdk"
)

// Manager is a struct that manages the pruning of old versions of the SC and SS.
type Manager struct {
	// scPruner is the pruner for the SC.
	scPruner sdkstore.Pruner
	// scPruningOption are the pruning options for the SC.
	scPruningOption *sdkstore.PruningOption
}

// NewManager creates a new Pruning Manager.
func NewManager(scPruner sdkstore.Pruner, scPruningOption *sdkstore.PruningOption) *Manager {
	return &Manager{
		scPruner:        scPruner,
		scPruningOption: scPruningOption,
	}
}

// Prune prunes the SC and SS to the provided version.
//
// NOTE: It can be called outside the store manually.
func (m *Manager) Prune(version uint64) error {
	// Prune the SC.
	if m.scPruningOption != nil {
		if prune, pruneTo := m.scPruningOption.ShouldPrune(version); prune {
			if err := m.scPruner.Prune(pruneTo); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Manager) signalPruning(pause bool) {
	if scPausablePruner, ok := m.scPruner.(sdkstore.PausablePruner); ok {
		scPausablePruner.PausePruning(pause)
	}
}

func (m *Manager) PausePruning() {
	m.signalPruning(true)
}

func (m *Manager) ResumePruning(version uint64) error {
	m.signalPruning(false)
	return m.Prune(version)
}

func (m *Manager) GetPruningOption() *sdkstore.PruningOption {
	return m.scPruningOption
}

func (m *Manager) SetPruningOption(opt sdkstore.PruningOption) {
	m.scPruningOption.KeepRecent = opt.KeepRecent
	m.scPruningOption.Interval = opt.Interval
}
