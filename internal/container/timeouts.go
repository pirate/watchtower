package container

import (
	"math"
	"time"
)

// DetachedCallTimeout is the existing bound for Docker recovery work: the
// stop timeout plus 30 seconds for API calls. It saturates rather than wrapping.
// A recovery phase starts this bound when it begins, not when the failed
// operation began, so recovery can extend the total transaction duration.
func DetachedCallTimeout(stop time.Duration) time.Duration {
	const callBudget = 30 * time.Second

	return min(max(stop, 0), math.MaxInt64-callBudget) + callBudget
}
