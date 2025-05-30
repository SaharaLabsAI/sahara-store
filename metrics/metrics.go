package metrics

import (
	"fmt"
	"time"

	"github.com/hashicorp/go-metrics"
)

var _ StoreMetrics = &Metrics{}

// StoreMetrics defines the set of supported metric APIs for the store package.
type StoreMetrics interface {
	IncrCounter(val float32, keys ...string)
	SetGauge(val float32, keys ...string)
	MeasureSince(start time.Time, keys ...string)
}

// Metrics defines a default StoreMetrics implementation.
type Metrics struct {
	Labels []metrics.Label
}

// IncrCounter implements StoreMetrics.
func (m *Metrics) IncrCounter(val float32, keys ...string) {
	metrics.IncrCounter(keys, val)
}

// SetGauge implements StoreMetrics.
func (m *Metrics) SetGauge(val float32, keys ...string) {
	metrics.SetGauge(keys, val)
}

// NewMetrics returns a new instance of the Metrics with labels set by the node
// operator.
func NewMetrics(labels [][]string) (*Metrics, error) {
	m := &Metrics{}

	if numGlobalLabels := len(labels); numGlobalLabels > 0 {
		parsedGlobalLabels := make([]metrics.Label, numGlobalLabels)
		for i, label := range labels {
			if len(label) != 2 {
				return &Metrics{}, fmt.Errorf("invalid global label length; expected 2, got %d", len(label))
			}

			parsedGlobalLabels[i] = metrics.Label{Name: label[0], Value: label[1]}
		}

		m.Labels = parsedGlobalLabels
	}

	return m, nil
}

// MeasureSince provides a wrapper functionality for emitting a time measure
// metric with global labels (if any).
func (m *Metrics) MeasureSince(start time.Time, keys ...string) {
	metrics.MeasureSinceWithLabels(keys, start.UTC(), m.Labels)
}

// NoOpMetrics is a no-op implementation of the StoreMetrics interface
type NoOpMetrics struct{}

// NewNoOpMetrics returns a new instance of the NoOpMetrics
func NewNoOpMetrics() NoOpMetrics {
	return NoOpMetrics{}
}

// MeasureSince is a no-op implementation of the StoreMetrics interface to avoid time.Now() calls
func (m NoOpMetrics) MeasureSince(start time.Time, keys ...string) {}

func (m NoOpMetrics) SetGauge(val float32, keys ...string) {}

func (m NoOpMetrics) IncrCounter(val float32, keys ...string) {}
