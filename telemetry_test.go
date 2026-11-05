package orbital_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

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

func TestMetadataCarrier(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(otel.GetTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})

	ctx, span := tp.Tracer("test").Start(t.Context(), "send")
	defer span.End()

	// inject span context into MetaData
	md := make(orbital.MetaData)
	otel.GetTextMapPropagator().Inject(ctx, orbital.NewMetadataCarrier(md))

	assert.NotEmpty(t, md, "traceparent should be written into MetaData")

	// extract on the receiver side and verify the trace is linked
	extracted := otel.GetTextMapPropagator().Extract(t.Context(), orbital.NewMetadataCarrier(md))
	sc := trace.SpanContextFromContext(extracted)

	require.True(t, sc.IsValid())
	assert.Equal(t, span.SpanContext().TraceID(), sc.TraceID(), "trace ID must be preserved across the queue")
	assert.Equal(t, span.SpanContext().SpanID(), sc.SpanID(), "span ID must be the parent's")
}
