package orbital

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
