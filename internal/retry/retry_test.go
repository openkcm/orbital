package retry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital/internal/retry"
)

func TestExponentialBackoffInterval(t *testing.T) {
	tests := []struct {
		baseInterval     int64
		maxIntervalLimit int64
		attempt          int64
		expected         int64
	}{
		{10, 20, 0, 10},
		{10, 20, 1, 20},
		{10, 20, 2, 20}, // Should cap at maxIntervalLimit
		{5, 100, 3, 40},
		{7, 50, 2, 28},
		{15, 60, 1, 30},
		{20, 100, 4, 100}, // Should cap at maxIntervalLimit
	}

	for _, test := range tests {
		result := retry.ExponentialBackoffInterval(test.baseInterval, test.maxIntervalLimit, test.attempt)
		assert.Equal(t, test.expected, result)
	}
}
