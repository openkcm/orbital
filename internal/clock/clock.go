package clock

import "time"

// Now returns the current time in UTC.
func Now() time.Time {
	return time.Now().UTC()
}

// NowUnixNano returns the current time in UTC as a Unix timestamp in nanoseconds.
func NowUnixNano() int64 {
	return time.Now().UTC().UnixNano()
}

// ToUnixNano converts a time.Time value to a Unix timestamp in nanoseconds.
func ToUnixNano(t time.Time) int64 {
	return t.UnixNano()
}
