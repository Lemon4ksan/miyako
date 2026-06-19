// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spinlock

import (
	"runtime"
	"sync/atomic"
)

// SpinLock implements a simple and fast spinlock using a busy-wait loop.
type SpinLock struct {
	state uint32
}

// Lock acquires the lock by spinning in a busy-wait loop.
func (s *SpinLock) Lock() {
	for !atomic.CompareAndSwapUint32(&s.state, 0, 1) {
		runtime.Gosched()
	}
}

// Unlock releases the lock.
func (s *SpinLock) Unlock() {
	atomic.StoreUint32(&s.state, 0)
}

// TryLock attempts to acquire the lock without waiting.
func (s *SpinLock) TryLock() bool {
	return atomic.CompareAndSwapUint32(&s.state, 0, 1)
}
