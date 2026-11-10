// Package game holds the flyer rules that are not workflow code.
package game

import (
	"hash/fnv"
	"strconv"
	"time"
)

const (
	DefaultDuration = 60 * time.Second
	MinDuration     = 15 * time.Second
	MaxDuration     = 120 * time.Second

	// MinGateInterval is the fastest a real flight can clear two gaps. It
	// stops a client from posting a winning score without flying.
	MinGateInterval = 280 * time.Millisecond

	// A treat adds between TreatBonusMin and TreatBonusMax to the durable
	// deadline. There is no cap on treats: the course spaces them further
	// apart as a flight goes on, and the cooldown stops a client from
	// posting pickups faster than the course can offer them.
	TreatBonusMin = 5 * time.Second
	TreatBonusMax = 10 * time.Second
	TreatCooldown = 4 * time.Second
)

// TreatBonus is the time one treat adds. It is a pure function of the
// session and the treat's number, so workflow code can call it and replay
// gets the same answer.
func TreatBonus(sessionID string, treat int) time.Duration {
	h := fnv.New32a()
	h.Write([]byte(sessionID + "/" + strconv.Itoa(treat)))
	steps := uint32((TreatBonusMax-TreatBonusMin)/time.Second) + 1
	return TreatBonusMin + time.Duration(h.Sum32()%steps)*time.Second
}
