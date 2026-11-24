package orbital_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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

// installOTel registers a recording TracerProvider and the W3C propagator globally.
// It restores the originals when the test ends.
func installOTel(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	origTP := otel.GetTracerProvider()
	origProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetTextMapPropagator(origProp)
	})
	return sr
}

func TestInjectTraceContext_NoActiveTrace(t *testing.T) {
	installOTel(t)
	tp, ts := orbital.InjectTraceContext(context.Background())
	assert.Empty(t, tp)
	assert.Empty(t, ts)
}

func TestInjectTraceContext_WithActiveSpan(t *testing.T) {
	installOTel(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "parent")
	defer span.End()

	tp, ts := orbital.InjectTraceContext(ctx)
	assert.NotEmpty(t, tp, "traceparent must be set when a span is active")
	_ = ts // tracestate may be empty
}

func TestStartLinkedSpan_EmptyTraceparent_NoLink(t *testing.T) {
	sr := installOTel(t)
	ctx, span := orbital.StartLinkedSpan(context.Background(), "child", "", "")
	span.End()

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "child", spans[0].Name())
	assert.Empty(t, spans[0].Links(), "no link expected when traceparent is empty")
	_ = ctx
}

func TestStartLinkedSpan_InvalidTraceparent_NoLink(t *testing.T) {
	sr := installOTel(t)
	ctx, span := orbital.StartLinkedSpan(context.Background(), "child", "not-a-valid-traceparent", "")
	span.End()

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Empty(t, spans[0].Links(), "no link expected for invalid traceparent")
	_ = ctx
}

func TestStartLinkedSpan_ValidTraceparent_HasLink(t *testing.T) {
	rec := installOTel(t)

	parentCtx, parentSpan := otel.Tracer("test").Start(context.Background(), "parent")
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(parentCtx, carrier)
	parentSpan.End()
	parentTraceparent := carrier.Get("traceparent")
	require.NotEmpty(t, parentTraceparent)

	_, childSpan := orbital.StartLinkedSpan(context.Background(), "child", parentTraceparent, "")
	childSpan.End()

	ended := rec.Ended()
	childIdx := -1
	for i, s := range ended {
		if s.Name() == "child" {
			childIdx = i
			break
		}
	}
	require.NotEqual(t, -1, childIdx, "child span not recorded")
	links := ended[childIdx].Links()
	require.Len(t, links, 1)

	wantTraceID := parentSpan.SpanContext().TraceID()
	assert.Equal(t, wantTraceID, links[0].SpanContext.TraceID(),
		"link must point to the parent trace")
}
