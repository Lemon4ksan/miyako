// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package scheduler provides a priority-queue task scheduler and execution
// rate limiters.
//
// It manages periodic and one-off tasks sorted by (NextRun, Priority) using a
// min-heap. Tasks with Interval > 0 are automatically re-enqueued after
// execution. The package also includes standalone [Debounce] and [Throttle]
// utilities for rate-limiting function calls.
//
// # Architecture
//
// The [Scheduler] uses a min-heap for O(log n) task insertion and extraction.
// A dedicated goroutine runs the event loop, sleeping until the next task's
// NextRun time. Dynamic wake-up via a channel avoids busy-waiting. Tasks are
// object-pooled via [sync.Pool] to minimize allocations.
//
// # Error Handling
//
// Task execution errors are returned from the [Task.Execute] function. The
// scheduler logs errors but does not stop - it continues processing the next
// task in the queue. Context cancellation of the scheduler gracefully shuts
// down the event loop.
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
//	    "github.com/lemon4ksan/miyako/scheduler"
//	)
//
//	func main() {
//	    s := scheduler.New()
//	    ctx, cancel := context.WithCancel(context.Background())
//	    defer cancel()
//
//	    go s.Start(ctx)
//
//	    t := s.AcquireTask()
//	    t.ID = "example-task"
//	    t.Priority = scheduler.PriorityNormal
//	    t.NextRun = time.Now().Add(50 * time.Millisecond)
//	    t.Execute = func(ctx context.Context) error {
//	        fmt.Println("Task executed!")
//	        return nil
//	    }
//
//	    s.Schedule(t)
//	    time.Sleep(100 * time.Millisecond)
//	}
package scheduler
