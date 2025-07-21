package retry

// ExponentialBackoffInterval calculates the backoff interval for a given attempt using exponential growth.
// baseIntervalSec: initial interval in seconds before the first retry.
// maxIntervalSec: maximum interval in seconds.
// attempts: current retry attempt number.
// Returns the interval in seconds, capped at maxIntervalSec.
func ExponentialBackoffInterval(baseIntervalSec, maxIntervalSec, attempts int64) int64 {
	interval := baseIntervalSec * int64(1) << attempts
	if interval > maxIntervalSec {
		return maxIntervalSec
	}
	return interval
}
