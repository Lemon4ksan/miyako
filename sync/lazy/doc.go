// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sync provides a thread-safe lazy initializer with reset support.
//
// Unlike [sync.Once], the cached value can be discarded via [Lazy.Reset],
// causing the next [Lazy.Get] to re-run the initialization function. This is
// useful for error recovery, configuration hot-reload, or any scenario where
// re-initialization is required.
//
// # Architecture
//
// The [Lazy] container caches the result (value + error) of its initialization
// function after the first call. All subsequent [Lazy.Get] calls return the
// cached result without re-executing the function. [Lazy.Reset] clears the
// cache and resets the done flag, allowing the next Get to re-initialize.
// All operations are protected by [sync.Mutex].
//
// # Error Handling
//
// If the initialization function returns an error, it is cached and returned
// on all subsequent Get calls until [Lazy.Reset] is called. This allows
// callers to retry initialization after fixing the underlying problem.
//
// # Example
//
//	package main
//
//	import (
//	    "fmt"
//
//	    "github.com/lemon4ksan/miyako/sync/lazy"
//	)
//
//	func main() {
//	    db := lazy.New(func() (string, error) {
//	        return "connected", nil
//	    })
//
//	    val, err := db.Get()
//	    fmt.Println(val, err) // connected <nil>
//
//	    db.Reset()
//
//	    val, err = db.Get()
//	    fmt.Println(val, err) // connected <nil> (re-initialized)
//	}
package lazy
