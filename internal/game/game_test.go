package game

import (
	"testing"
	"time"
)

func TestDurations(t *testing.T) {
	if MinDuration > DefaultDuration || DefaultDuration > MaxDuration {
		t.Fatalf("duration bounds %s %s %s", MinDuration, DefaultDuration, MaxDuration)
	}
	if MinGateInterval < 200*time.Millisecond {
		t.Fatalf("gate interval %s would reject a real flight", MinGateInterval)
	}
}

func TestTreatBonusStaysInRangeAndVaries(t *testing.T) {
	seen := map[time.Duration]bool{}
	for treat := 1; treat <= 200; treat++ {
		b := TreatBonus("abcd1234", treat)
		if b < TreatBonusMin || b > TreatBonusMax || b%time.Second != 0 {
			t.Fatalf("treat %d bonus %s", treat, b)
		}
		if b != TreatBonus("abcd1234", treat) {
			t.Fatalf("treat %d bonus is not stable", treat)
		}
		seen[b] = true
	}
	if len(seen) != 6 {
		t.Fatalf("bonuses covered %d of 6 values", len(seen))
	}
}
