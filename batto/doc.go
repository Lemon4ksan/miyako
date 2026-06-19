// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package batto provides generic duplicate call suppression with context
// awareness and panic isolation.
//
// It solves the "thundering herd" problem by ensuring that duplicate concurrent
// requests with the same key execute only once. Subsequent callers wait for the
// active execution to complete and receive the same result.
//
// # Architecture
//
// The [Group] is the central duplicate call suppressor. It maintains an internal
// map of in-flight calls keyed by [Group.Do]'s key parameter. When a new request
// arrives for an already-in-flight key, the caller blocks until the original
// worker completes, then receives the shared result.
//
// # Panic Isolation
//
// If a worker function panics, the panic is re-propagated only to the goroutine
// that initiated the call. All secondary waiters receive [ErrWorkerPanicked]
// instead, preventing a single panicking worker from crashing the entire pool.
//
// # Context Cancellation
//
// If all waiting contexts are cancelled, the worker context is cancelled
// immediately to free resources. This prevents zombie goroutines from leaking
// when upstream callers give up.
//
// # Example
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//	    "sync"
//	    "sync/atomic"
//	    "time"
//
//	    "github.com/lemon4ksan/miyako/batto"
//	)
//
//	func main() {
//	    g := &batto.Group[string, string]{}
//	    ctx := context.Background()
//
//	    var callCounter int32
//	    var wg sync.WaitGroup
//
//	    fn := func(workerCtx context.Context) (string, error) {
//	        atomic.AddInt32(&callCounter, 1)
//	        time.Sleep(50 * time.Millisecond)
//	        return "shared-value", nil
//	    }
//
//	    for i := range 5 {
//	        wg.Add(1)
//	        go func(id int) {
//	            defer wg.Done()
//	            res, err := g.Do(ctx, "my-key", fn)
//	            if err != nil {
//	                fmt.Printf("Worker %d failed: %v\n", id, err)
//	                return
//	            }
//	            fmt.Printf("Worker %d got: %s\n", id, res)
//	        }(i)
//	    }
//
//	    wg.Wait()
//	    fmt.Printf("Total function executions: %d\n", atomic.LoadInt32(&callCounter))
//	}
package batto
