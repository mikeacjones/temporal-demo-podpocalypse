// Package game holds the flyer rules that are not workflow code.
package game

import "time"

const (
	DefaultDuration = 60 * time.Second
	MinDuration     = 15 * time.Second
	MaxDuration     = 120 * time.Second

	// MinGateInterval is the fastest a real flight can clear two gaps. It
	// stops a client from posting a winning score without flying.
	MinGateInterval = 280 * time.Millisecond

	// A treat adds time to the durable deadline. The cap keeps a booth
	// round from running away if the client posts every pickup.
	TreatBonus    = 8 * time.Second
	TreatCooldown = 4 * time.Second
	MaxTreats     = 4
)
