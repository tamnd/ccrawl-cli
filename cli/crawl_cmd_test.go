package cli

import (
	"testing"

	"github.com/tamnd/ccrawl-cli/ccrawl"
)

// TestSeedTierFloorIsTheBestASeedCanDo pins the two constants to each other.
// The floor is a claim about the tier function, not a preference: at the change
// rate a seed assumes, no rank in the table reaches tier 1, so asking for tier 1
// can only read 262 million rows and emit nothing. If someone raises
// seedChangeRate past the 0.8 that tier 1 wants, or the tier thresholds move,
// the floor is wrong and the command starts refusing work it could do.
func TestSeedTierFloorIsTheBestASeedCanDo(t *testing.T) {
	best := 5
	for _, pos := range []int64{1, 2, 100, 99_999, 100_000, 100_001, 1_000_000, 5_000_000, 10_000_000, 262_000_000} {
		if tier := ccrawl.CrawlTier(pos, seedChangeRate); tier < best {
			best = tier
		}
	}
	if best != seedTierFloor {
		t.Errorf("the best tier a seed reaches is %d, but seedTierFloor says %d", best, seedTierFloor)
	}
}
