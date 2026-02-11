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

// TimeFromUnixNano converts a Unix timestamp in nanoseconds to a time.Time value in UTC.
func TimeFromUnixNano(nano int64) time.Time {
	return time.Unix(0, nano).UTC()
}
