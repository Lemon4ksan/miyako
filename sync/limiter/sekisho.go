// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package limiter

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ErrClosed is returned when operations are performed on a closed KeyedLimiter.
var ErrClosed = errors.New("limiter is closed")

type limiterEntry struct {
	limiter   *rate.Limiter
	lastTouch time.Time
}

// KeyedLimiter manages dynamic rate limiters per key, automatically cleaning up
// inactive limiters from memory after a configured TTL duration.
type KeyedLimiter[K comparable] struct {
	mu       sync.Mutex
	r        rate.Limit
	b        int
	ttl      time.Duration
	limiters map[K]*limiterEntry
	closeCh  chan struct{}
	wg       sync.WaitGroup
	closed   bool
}

// NewKeyedLimiter creates a new KeyedLimiter with the specified rate limit, burst size,
// and TTL for inactive limiters. It starts a background sweeper goroutine to clean up
// expired entries.
func NewKeyedLimiter[K comparable](r rate.Limit, b int, ttl time.Duration) *KeyedLimiter[K] {
	kl := &KeyedLimiter[K]{
		r:        r,
		b:        b,
		ttl:      ttl,
		limiters: make(map[K]*limiterEntry),
		closeCh:  make(chan struct{}),
	}

	kl.wg.Go(kl.sweepLoop)

	return kl
}

func (kl *KeyedLimiter[K]) sweepLoop() {
	sweepInterval := max(kl.ttl/2, time.Millisecond)

	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-kl.closeCh:
			return
		case <-ticker.C:
			kl.sweep()
		}
	}
}

func (kl *KeyedLimiter[K]) sweep() {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	now := time.Now()

	for k, entry := range kl.limiters {
		if now.Sub(entry.lastTouch) > kl.ttl {
			delete(kl.limiters, k)
		}
	}
}

// getLimiter retrieves or creates a rate.Limiter for the given key.
func (kl *KeyedLimiter[K]) getLimiter(key K) (*rate.Limiter, error) {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	if kl.closed {
		return nil, ErrClosed
	}

	entry, ok := kl.limiters[key]
	if !ok {
		entry = &limiterEntry{
			limiter: rate.NewLimiter(kl.r, kl.b),
		}
		kl.limiters[key] = entry
	}

	entry.lastTouch = time.Now()

	return entry.limiter, nil
}

// Allow reports whether an event may occur for the given key immediately.
func (kl *KeyedLimiter[K]) Allow(key K) (bool, error) {
	lim, err := kl.getLimiter(key)
	if err != nil {
		return false, err
	}

	return lim.Allow(), nil
}

// Wait blocks until the rate limiter allows an event to occur for the key,
// or until the context is cancelled.
func (kl *KeyedLimiter[K]) Wait(ctx context.Context, key K) error {
	lim, err := kl.getLimiter(key)
	if err != nil {
		return err
	}

	return lim.Wait(ctx)
}

// Close stops the background sweeper and marks the limiter as closed.
func (kl *KeyedLimiter[K]) Close() error {
	kl.mu.Lock()
	if kl.closed {
		kl.mu.Unlock()
		return nil
	}

	kl.closed = true

	close(kl.closeCh)
	kl.mu.Unlock()

	kl.wg.Wait()

	return nil
}
