// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package limiter

import (
	"context"
	"sync"
	"time"
)

// AdaptiveLimiter controls concurrency dynamically using a Vegas-style congestion algorithm.
// It measures round-trip times (RTT) to dynamically adjust the allowed concurrent request limit.
type AdaptiveLimiter struct {
	mu          sync.Mutex
	limit       float64
	minLimit    float64
	maxLimit    float64
	alpha, beta float64
	active      int
	waitChs     []chan struct{}
	minRTT      time.Duration
	smoothedRTT time.Duration
	lastReset   time.Time
}

// NewAdaptiveLimiter initializes an [AdaptiveLimiter] instance with default settings.
func NewAdaptiveLimiter(initialLimit float64) *AdaptiveLimiter {
	return &AdaptiveLimiter{
		limit:     initialLimit,
		minLimit:  1.0,
		maxLimit:  1000.0,
		alpha:     2.0,
		beta:      5.0,
		lastReset: time.Now(),
	}
}

// Acquire blocks until a concurrent execution slot becomes available or context is cancelled.
// It returns [context.Canceled] or [context.DeadlineExceeded] if the context expires.
func (l *AdaptiveLimiter) Acquire(ctx context.Context) error {
	l.mu.Lock()
	if l.active < int(l.limit) {
		l.active++
		l.mu.Unlock()
		return nil
	}

	ch := make(chan struct{})
	l.waitChs = append(l.waitChs, ch)
	l.mu.Unlock()

	select {
	case <-ctx.Done():
		l.mu.Lock()
		for i, w := range l.waitChs {
			if w == ch {
				l.waitChs = append(l.waitChs[:i], l.waitChs[i+1:]...)
				break
			}
		}

		l.mu.Unlock()

		return ctx.Err()

	case <-ch:
		return nil
	}
}

// Release registers request completion, updates RTT metrics, and recalculates limits.
// It adjusts the concurrency limit based on Vegas queuing limits (alpha and beta thresholds).
func (l *AdaptiveLimiter) Release(rtt time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.active--

	if time.Since(l.lastReset) > 30*time.Second {
		l.minRTT = 0
		l.lastReset = time.Now()
	}

	if l.minRTT == 0 || rtt < l.minRTT {
		l.minRTT = rtt
	}

	if l.smoothedRTT == 0 {
		l.smoothedRTT = rtt
	} else {
		l.smoothedRTT = time.Duration(0.9*float64(l.smoothedRTT) + 0.1*float64(rtt))
	}

	queue := l.limit * (1.0 - float64(l.minRTT)/float64(l.smoothedRTT))

	if queue > l.beta {
		l.limit = max(l.minLimit, l.limit-1.0)
	} else if queue < l.alpha {
		l.limit = min(l.maxLimit, l.limit+1.0)
	}

	slots := int(l.limit) - l.active
	for slots > 0 && len(l.waitChs) > 0 {
		ch := l.waitChs[0]
		l.waitChs = l.waitChs[1:]

		close(ch)

		l.active++
		slots--
	}
}

// Limit returns the active dynamic concurrency limit.
func (l *AdaptiveLimiter) Limit() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}
