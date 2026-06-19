// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lazy

import (
	"sync"
)

// Lazy implements a thread-safe lazy initializer with reset support.
type Lazy[T any] struct {
	mu    sync.Mutex
	init  func() (T, error)
	value T
	err   error
	done  bool
}

// New creates a new Lazy instance with the given initialization function.
func New[T any](init func() (T, error)) *Lazy[T] {
	return &Lazy[T]{init: init}
}

// Get returns the value. If it has not been initialized yet,
// the method calls the initialization function. The result (including error) is cached.
func (l *Lazy[T]) Get() (T, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.done {
		return l.value, l.err
	}

	l.value, l.err = l.init()
	l.done = true

	return l.value, l.err
}

// Reset resets the cached state.
// The next call to Get() will restart the initialization function.
func (l *Lazy[T]) Reset() {
	l.mu.Lock()
	l.done = false

	var zero T

	l.value = zero
	l.err = nil
	l.mu.Unlock()
}
