package orbital

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const meterName = "github.com/openkcm/orbital"

type MetricCollector interface {
	RecordOperation(ctx context.Context, op string, d time.Duration, failed bool, attrs ...attribute.KeyValue)
	RecordError(ctx context.Context, op string, attrs ...attribute.KeyValue)
}

type metricsCollector struct {
	duration metric.Float64Histogram
	errors   metric.Int64Counter
}

func NewMetricCollector(provider metric.MeterProvider) (MetricCollector, error) {
	meter := provider.Meter(meterName)

	duration, err := meter.Float64Histogram(
		"messaging.operation.duration",
		metric.WithDescription("Duration of orbital transport operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	errors, err := meter.Int64Counter(
		"messaging.operation.errors",
		metric.WithDescription("Number of failed orbital transport operations"),
	)
	if err != nil {
		return nil, err
	}

	return &metricsCollector{duration: duration, errors: errors}, nil
}

func (c *metricsCollector) RecordError(ctx context.Context, op string, attrs ...attribute.KeyValue) {
	all := append([]attribute.KeyValue{attribute.String("messaging.operation.name", op)}, attrs...)
	c.errors.Add(ctx, 1, metric.WithAttributes(all...))
}

func (c *metricsCollector) RecordOperation(ctx context.Context, op string, d time.Duration, failed bool, attrs ...attribute.KeyValue) {
	all := append([]attribute.KeyValue{attribute.String("messaging.operation.name", op)}, attrs...)
	c.duration.Record(ctx, d.Seconds(), metric.WithAttributes(all...))
	if failed {
		c.RecordError(ctx, op, attrs...)
	}
}

// metadataCarrier adapts MetaData to the otel TextMapCarrier interface.
type metadataCarrier MetaData

func (m metadataCarrier) Get(key string) string {
	return m[key]
}

func (m metadataCarrier) Set(key, val string) {
	m[key] = val
}

func (m metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// InjectTraceContext extracts the active span's W3C traceparent and tracestate from ctx.
// Both values are empty when no span is active. Use LabelTraceparent / LabelTracestate as
// the job label keys when storing the returned values.
func InjectTraceContext(ctx context.Context) (string, string) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get(LabelTraceparent), carrier.Get(LabelTracestate)
}

// StartLinkedSpan starts a new span whose link points to the trace identified by
// traceparent and tracestate. When traceparent is empty or invalid no link is added.
// The caller is responsible for ending the returned span.
func StartLinkedSpan(ctx context.Context, name, traceparent, tracestate string) (context.Context, trace.Span) {
	var opts []trace.SpanStartOption
	if link, ok := extractSpanLink(traceparent, tracestate); ok {
		opts = append(opts, trace.WithLinks(link))
	}
	ctx, span := otel.GetTracerProvider().Tracer("").Start(ctx, name, opts...) //nolint:spancheck // returned to caller
	return ctx, span                                                           //nolint:spancheck // caller owns the span
}

func extractSpanLink(traceparent, tracestate string) (trace.Link, bool) {
	if traceparent == "" {
		return trace.Link{}, false
	}
	carrier := propagation.MapCarrier{LabelTraceparent: traceparent, LabelTracestate: tracestate}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	spanCtx := trace.SpanContextFromContext(ctx)
	if !spanCtx.IsValid() {
		return trace.Link{}, false
	}
	return trace.Link{SpanContext: spanCtx}, true
}
