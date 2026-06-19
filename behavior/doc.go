// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package behavior provides a generic orchestrator for managing concurrent tasks.
//
// It is designed for running multiple independent background tasks (behaviors)
// in parallel with graceful shutdown support. Unlike [lifecycle.Service],
// behaviors have a simplified contract: they start immediately and run until
// the context is canceled.
//
// # Architecture
//
// The [Orchestrator] manages the lifecycle of registered [Behavior] instances.
// Each behavior runs in its own goroutine and is tracked for graceful shutdown.
// The orchestrator supports optional configuration via [WithLogger] and
// [WithFailFast] options.
//
// # Error Handling
//
// By default, when a behavior fails, the error is logged but other behaviors
// continue running. Use [WithFailFast] to cancel all behaviors on the first error.
//
// # Example
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//	    "time"
//
//	    "github.com/lemon4ksan/miyako/behavior"
//	)
//
//	type tickerBehavior struct {
//	    name string
//	    interval time.Duration
//	}
//
//	func (t *tickerBehavior) Name() string { return t.name }
//
//	func (t *tickerBehavior) Run(ctx context.Context) error {
//	    ticker := time.NewTicker(t.interval)
//	    defer ticker.Stop()
//
//	    for {
//	        select {
//	        case <-ctx.Done():
//	            return nil
//	        case <-ticker.C:
//	            fmt.Printf("Tick from %s\n", t.name)
//	        }
//	    }
//	}
//
//	func main() {
//	    orch := behavior.NewOrchestrator()
//
//	    orch.Register(&tickerBehavior{name: "fast", interval: time.Second})
//	    orch.Register(&tickerBehavior{name: "slow", interval: 5 * time.Second})
//
//	    ctx, cancel := context.WithCancel(context.Background())
//	    defer cancel()
//
//	    if err := orch.Start(ctx); err != nil {
//	        panic(err)
//	    }
//
//	    // Run for 10 seconds then stop
//	    time.Sleep(10 * time.Second)
//	    orch.Stop()
//	}
package behavior
