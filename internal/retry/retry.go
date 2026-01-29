package retry

// ExponentialBackoffInterval calculates the backoff interval for a given attempt using exponential growth.
// baseIntervalSec: initial interval in seconds before the first retry.
// maxIntervalSec: maximum interval in seconds.
// attempts: current retry attempt number.
// Returns the interval in seconds, capped at maxIntervalSec.
func ExponentialBackoffInterval(baseIntervalSec, maxIntervalSec, attempts uint64) uint64 {
	interval := baseIntervalSec << attempts
	if interval < baseIntervalSec || interval == 0 {
		return maxIntervalSec
	}
	return min(interval, maxIntervalSec)
}
