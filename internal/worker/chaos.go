package worker

import "math/rand/v2"

// chance reports true with probability p. It is the single entry point for the
// worker's fault-injection knobs, so that a build with every rate at zero behaves
// exactly like one with no chaos code at all.
func chance(p float64) bool {
	if p <= 0 {
		return false
	}
	if p >= 1 {
		return true
	}
	return rand.Float64() < p
}
