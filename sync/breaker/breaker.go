// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package breaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned when the circuit breaker is in the Open state.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// State represents the operational state of a CircuitBreaker.
type State int

const (
	// StateClosed allows requests to flow normally.
	StateClosed State = iota
	// StateOpen fails requests immediately.
	StateOpen
	// StateHalfOpen allows a single test request to check downstream health.
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "Closed"
	case StateOpen:
		return "Open"
	case StateHalfOpen:
		return "Half-Open"
	default:
		return "Unknown"
	}
}

type record struct {
	t        time.Time
	isFailed bool
}

// Config defines the operational parameters for a CircuitBreaker.
type Config struct {
	// FailureThreshold is the ratio of failures (0.0 to 1.0) that triggers StateOpen.
	FailureThreshold float64
	// Cooldown is the duration spent in StateOpen before transitioning to StateHalfOpen.
	Cooldown time.Duration
	// MinRequests is the minimum number of requests in a Window before threshold check is active.
	MinRequests int
	// Window is the sliding time duration over which failures are tracked.
	Window time.Duration
	// OnStateChange is an optional callback executed on state transitions.
	OnStateChange func(from, to State)
}

func (c *Config) resolveDefaults() {
	if c.FailureThreshold <= 0 || c.FailureThreshold > 1.0 {
		c.FailureThreshold = 0.5
	}

	if c.Cooldown <= 0 {
		c.Cooldown = 5 * time.Second
	}

	if c.MinRequests <= 0 {
		c.MinRequests = 5
	}

	if c.Window <= 0 {
		c.Window = 10 * time.Second
	}
}

// CircuitBreaker prevents cascading failures by failing fast when downstream services are unhealthy.
type CircuitBreaker[T any] struct {
	mu                sync.Mutex
	cfg               Config
	state             State
	openTime          time.Time
	records           []record
	halfOpenExecuting bool
}

// New creates and returns a new CircuitBreaker instance with the given Config.
func New[T any](cfg Config) *CircuitBreaker[T] {
	cfg.resolveDefaults()

	return &CircuitBreaker[T]{
		cfg:   cfg,
		state: StateClosed,
	}
}

// Do wraps the execution of a function fn, tracking its success or failure.
// If the breaker is open, it returns [ErrCircuitOpen] immediately without running fn.
func (cb *CircuitBreaker[T]) Do(ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	cb.mu.Lock()

	now := time.Now()

	cb.checkState(now)

	if cb.state == StateOpen {
		cb.mu.Unlock()

		var zero T

		return zero, ErrCircuitOpen
	}

	if cb.state == StateHalfOpen {
		if cb.halfOpenExecuting {
			cb.mu.Unlock()

			var zero T

			return zero, ErrCircuitOpen
		}

		cb.halfOpenExecuting = true
	}

	isHalfOpen := cb.state == StateHalfOpen

	cb.mu.Unlock()

	val, err := fn(ctx)

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if isHalfOpen {
		cb.halfOpenExecuting = false

		if err != nil {
			cb.transitionTo(StateOpen, time.Now())
		} else {
			cb.transitionTo(StateClosed, time.Time{})
		}

		return val, err
	}

	cb.records = append(cb.records, record{t: time.Now(), isFailed: err != nil})

	cb.pruneRecords(time.Now())

	if len(cb.records) >= cb.cfg.MinRequests {
		failures := 0
		for _, r := range cb.records {
			if r.isFailed {
				failures++
			}
		}

		ratio := float64(failures) / float64(len(cb.records))
		if ratio >= cb.cfg.FailureThreshold {
			cb.transitionTo(StateOpen, time.Now())
		}
	}

	return val, err
}

func (cb *CircuitBreaker[T]) checkState(now time.Time) {
	if cb.state == StateOpen && now.Sub(cb.openTime) > cb.cfg.Cooldown {
		cb.transitionTo(StateHalfOpen, time.Time{})
	}
}

func (cb *CircuitBreaker[T]) transitionTo(target State, openTime time.Time) {
	if cb.state == target {
		return
	}

	from := cb.state
	cb.state = target
	cb.openTime = openTime

	if target == StateClosed || target == StateOpen {
		cb.records = nil
	}

	if cb.cfg.OnStateChange != nil {
		go cb.cfg.OnStateChange(from, target)
	}
}

func (cb *CircuitBreaker[T]) pruneRecords(now time.Time) {
	cutoff := now.Add(-cb.cfg.Window)
	i := 0

	for i < len(cb.records) && cb.records[i].t.Before(cutoff) {
		i++
	}

	if i > 0 {
		cb.records = cb.records[i:]
	}
}

// State returns the current State of the CircuitBreaker.
func (cb *CircuitBreaker[T]) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.checkState(time.Now())

	return cb.state
}
