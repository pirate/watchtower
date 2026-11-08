package container

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Pure duration arithmetic needs no fake clock, daemon, or client.
func TestStopRecoveryTimeout(t *testing.T) {
	for _, test := range []struct {
		name       string
		fallback   time.Duration
		configured *int
		want       time.Duration
	}{
		{name: "default", fallback: 10 * time.Second, want: 40 * time.Second},
		{name: "container override", fallback: 10 * time.Second, configured: new(90), want: 120 * time.Second},
		{name: "shorter override retains default bound", fallback: 10 * time.Second, configured: new(0), want: 40 * time.Second},
		{name: "negative override ignored", fallback: 10 * time.Second, configured: new(-1), want: 40 * time.Second},
		{name: "negative default bounded", fallback: -1, want: 30 * time.Second},
		{name: "default saturation", fallback: time.Duration(math.MaxInt64), want: time.Duration(math.MaxInt64)},
	} {
		t.Run(test.name, func(t *testing.T) { require.Equal(t, test.want, stopRecoveryTimeout(test.fallback, test.configured)) })
	}
	// Docker's stop seconds use int. Only a 64-bit int can exceed the largest
	// time.Duration in seconds; avoid overflowing the test on 32-bit platforms.
	limitSeconds := int64(math.MaxInt64) / int64(time.Second)
	if int64(int(limitSeconds)) == limitSeconds {
		oversized := int(limitSeconds) + 1
		require.Equal(t, time.Duration(math.MaxInt64), stopRecoveryTimeout(10*time.Second, &oversized))
	}
}
