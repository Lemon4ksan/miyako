// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package keylock provides a generic, thread-safe, striped/key-based locking
// mechanism.
//
// It allows separate goroutines to concurrently lock and work with different
// keys, preventing global application lock bottlenecks. Unlike a
// map[string]*sync.Mutex, keys are automatically cleaned up via reference
// counting when no goroutine holds them.
//
// # Architecture
//
// The [KeyMutex] stores per-key lock state in a map protected by [sync.RWMutex].
// Each key entry tracks a reference count of waiting/holding goroutines. When
// the count drops to zero, the entry is removed from the map, preventing memory
// leaks in long-running applications with dynamic key sets.
//
// # Error Handling
//
// [KeyMutex.Unlock] panics if the key is not currently locked by the calling
// goroutine. This catches double-unlock and unlock-without-lock bugs at the
// point of failure rather than silently corrupting state.
//
// # Example
//
//	package main
//
//	import (
//	    "fmt"
//	    "sync"
//	    "time"
//
//	    "github.com/lemon4ksan/miyako/sync/keylock"
//	)
//
//	func main() {
//	    kl := keylock.New[string]()
//	    var wg sync.WaitGroup
//
//	    wg.Add(2)
//
//	    go func() {
//	        defer wg.Done()
//	        kl.Lock("user-alice")
//	        defer kl.Unlock("user-alice")
//	        fmt.Println("Locked alice")
//	        time.Sleep(50 * time.Millisecond)
//	    }()
//
//	    go func() {
//	        defer wg.Done()
//	        kl.Lock("user-bob")
//	        defer kl.Unlock("user-bob")
//	        fmt.Println("Locked bob - no wait for alice")
//	        time.Sleep(50 * time.Millisecond)
//	    }()
//
//	    wg.Wait()
//	}
package keylock
