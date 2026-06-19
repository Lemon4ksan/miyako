// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package semaphore

import (
	"context"
	"sync"
)

// Semaphore is a semaphore that dynamically adjusts its limit on the fly.
type Semaphore struct {
	mu      sync.Mutex
	limit   int
	active  int
	waiters []chan struct{}
}

// New creates a new Semaphore with the given initial limit.
func New(initialLimit int) *Semaphore {
	return &Semaphore{
		limit: initialLimit,
	}
}

// Acquire attempts to acquire a slot from the semaphore, blocking if the limit is exceeded.
func (s *Semaphore) Acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	if s.active < s.limit {
		s.active++
		s.mu.Unlock()
		return nil
	}

	ch := make(chan struct{})
	s.waiters = append(s.waiters, ch)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		s.mu.Lock()
		for i, w := range s.waiters {
			if w == ch {
				s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
				break
			}
		}

		s.mu.Unlock()

		return ctx.Err()

	case <-ch:
		return nil
	}
}

// Release releases a slot from the semaphore.
func (s *Semaphore) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.active--
	s.notifyWaiters()
}

// Resize dynamically adjusts the limit of the semaphore on the fly.
func (s *Semaphore) Resize(newLimit int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.limit = newLimit
	s.notifyWaiters()
}

func (s *Semaphore) notifyWaiters() {
	for s.active < s.limit && len(s.waiters) > 0 {
		ch := s.waiters[0]
		s.waiters = s.waiters[1:]
		s.active++

		close(ch)
	}
}
