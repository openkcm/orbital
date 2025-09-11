package retry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital/internal/retry"
)

func TestExponentialBackoffInterval(t *testing.T) {
	tests := []struct {
		testName         string
		baseInterval     int64
		maxIntervalLimit int64
		attempt          int64
		expected         int64
	}{
		{"should return baseInterval for attempt 0", 20, 20, 0, 20},
		{"should return exponential backoff for attempt 1", 10, 20, 1, 20},
		{"should cap at maxIntervalLimit if the exponential backoff exceeds it", 10, 20, 2, 20},
		{"should cap at maxIntervalLimit when overflow results in negative value", 10, 10240, 60, 10240},
		{"should cap at maxIntervalLimit when overflow results in zero value", 99999999, 10240, 9999, 10240},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			result := retry.ExponentialBackoffInterval(test.baseInterval, test.maxIntervalLimit, test.attempt)
			assert.Equal(t, test.expected, result)
		})
	}
}
