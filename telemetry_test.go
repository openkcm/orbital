package orbital_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/openkcm/orbital"
)

func TestMetricCollector(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	collector, err := orbital.NewMetricCollector(provider)
	require.NoError(t, err)

	ctx := t.Context()

	collector.RecordOperation(ctx, "SendTaskRequest", 10*time.Millisecond, false)
	collector.RecordOperation(ctx, "SendTaskRequest", 10*time.Millisecond, true) // error
	collector.RecordOperation(ctx, "ReceiveTaskResponse", 20*time.Millisecond, false)
	collector.RecordOperation(ctx, "Process", 5*time.Millisecond, true) // error
	collector.RecordError(ctx, "MaxReconcileExceeded")
	collector.RecordError(ctx, "TargetNotFound")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	var totalDurationPoints uint64
	var totalErrorCount int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					totalDurationPoints += dp.Count
				}
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					totalErrorCount += dp.Value
				}
			}
		}
	}

	assert.Equal(t, uint64(4), totalDurationPoints)
	assert.Equal(t, int64(4), totalErrorCount)
}
