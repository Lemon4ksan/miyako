// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package spinlock provides a lightweight CAS-based spin lock.
//
// It is designed for very short critical sections where the cost of goroutine
// parking and unparking (as in [sync.Mutex]) exceeds the cost of spinning.
// The lock uses [sync/atomic.CompareAndSwapUint32] with [runtime.Gosched] to
// yield the processor between spins, avoiding busy-starvation.
//
// # Architecture
//
// The [SpinLock] uses a single uint32 atomically swapped between 0 (unlocked)
// and 1 (locked). [Lock] spins via CAS, calling [runtime.Gosched] on each
// failed attempt to yield the processor. [TryLock] attempts a single CAS
// without spinning. Zero value is ready to use.
//
// # When to Use
//
// Use SpinLock when the critical section is very short (nanoseconds to low
// microseconds), contention is low, and the lock is rarely held for long.
// For longer critical sections or high contention, prefer [sync.Mutex] which
// parks the goroutine instead of burning CPU cycles.
//
// # Example
//
//	package main
//
//	import (
//	    "fmt"
//
//	    "github.com/lemon4ksan/miyako/sync/spinlock"
//	)
//
//	func main() {
//	    var mu spinlock.SpinLock
//
//	    mu.Lock()
//	    fmt.Println("locked")
//	    mu.Unlock()
//
//	    if mu.TryLock() {
//	        fmt.Println("try-lock succeeded")
//	        mu.Unlock()
//	    }
//	}
package spinlock
