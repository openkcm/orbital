package clock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital/internal/clock"
)

func TestNowUTC(t *testing.T) {
	now := clock.Now()
	assert.NotZero(t, now, "should return a non-zero time")
	assert.True(t, now.Equal(now.UTC()), "should return time in UTC")
	assert.Equal(t, time.UTC, now.Location(), "should have UTC location")
}

func TestNowUnixNano(t *testing.T) {
	now := clock.NowUnixNano()
	assert.NotZero(t, now, "should return a non-zero timestamp")

	delta := time.Now().UTC().UnixNano() - now
	assert.LessOrEqual(t, delta, int64(time.Millisecond), "timestamp should be within 1ms of current time")
}

func TestToUnixNano(t *testing.T) {
	now := time.Now()
	unixNano := clock.ToUnixNano(now)
	assert.NotZero(t, unixNano, "should return a non-zero timestamp")
	assert.Equal(t, now.UnixNano(), unixNano, "should convert time to UnixNano correctly")
}

func TestTimeFromUnixNano(t *testing.T) {
	unixNano := time.Now().UnixNano()
	convertedTime := clock.TimeFromUnixNano(unixNano)
	assert.Equal(t, time.UTC, convertedTime.Location())
	assert.Equal(t, unixNano, convertedTime.UnixNano())
}
