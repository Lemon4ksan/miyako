// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package limiter provides dynamic concurrency rate limiters and key-based rate limiters with auto-cleanup.
//
// The package offers two main types:
//
//   - [AdaptiveLimiter]: Controls concurrency dynamically using a Vegas-style congestion
//     control algorithm. It is designed to prevent overload in distributed systems by dynamically
//     adjusting the concurrent request limit based on observed response times (RTT).
//
//   - [KeyedLimiter]: Manages dynamic rate limiters per key (e.g. per client IP or user ID),
//     automatically cleaning up inactive limiters from memory after a configured TTL of inactivity.
//
// # Vegas Congestion Algorithm
//
// The limiter measures round-trip times (RTT) for requests to estimate the size of
// the queue at the server. It calculates:
//
//	queue = limit * (1.0 - minRTT / smoothedRTT)
//
// Where:
//   - minRTT is the baseline minimum RTT seen within a sliding 30-second window.
//   - smoothedRTT is the Exponential Moving Average (EMA) of RTTs:
//     smoothedRTT = 0.9 * smoothedRTT + 0.1 * RTT
//
// The limit is then adjusted dynamically based on the queue size compared against
// alpha and beta thresholds:
//   - If queue < alpha (default 2.0), the system is underutilized and the limit is increased.
//   - If queue > beta (default 5.0), the system is experiencing congestion and the limit is decreased.
//   - Otherwise (alpha <= queue <= beta), the limit remains unchanged.
//
// # Keyed Limiter
//
// [KeyedLimiter] manages dynamic rate limiters per key. It dynamically allocates standard
// [golang.org/x/time/rate.Limiter] instances for active keys and automatically sweeps them
// from memory after a configured TTL duration of inactivity.
//
// A background sweeper goroutine runs periodically (with an interval of TTL / 2) to clean up
// inactive entries. Calling [KeyedLimiter.Close] stops this sweeper and releases its resources.
//
// # Error Handling
//
// [AdaptiveLimiter.Acquire] respects context cancellation. If a context is cancelled
// or times out while waiting for a slot, the goroutine is safely unblocked, its waiter
// channel is removed from the internal queue, and the context's error is returned.
//
// [KeyedLimiter.Allow] and [KeyedLimiter.Wait] will return [ErrClosed] if they are called
// after the limiter has been closed. [KeyedLimiter.Wait] also respects context cancellation.
//
// # Examples
//
// ## AdaptiveLimiter Example
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//	    "time"
//
//	    "github.com/lemon4ksan/miyako/sync/limiter"
//	)
//
//	func main() {
//	    // Start with a concurrency limit of 10.0
//	    l := limiter.NewAdaptiveLimiter(10.0)
//
//	    ctx := context.Background()
//	    start := time.Now()
//
//	    // Acquire a concurrency slot
//	    if err := l.Acquire(ctx); err != nil {
//	        panic(err)
//	    }
//
//	    // Perform some work...
//	    time.Sleep(50 * time.Millisecond)
//
//	    // Release the slot, passing the actual round-trip time (RTT)
//	    rtt := time.Since(start)
//	    l.Release(rtt)
//
//	    fmt.Printf("Current dynamic limit: %f\n", l.Limit())
//	}
//
// ## KeyedLimiter Example
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//	    "time"
//
//	    "golang.org/x/time/rate"
//	    "github.com/lemon4ksan/miyako/sync/limiter"
//	)
//
//	func main() {
//	    // Create a keyed rate limiter with a limit of 5 requests per second,
//	    // burst size of 10, and a TTL of 5 minutes for inactive keys.
//	    kl := limiter.NewKeyedLimiter[string](rate.Limit(5), 10, 5*time.Minute)
//	    defer kl.Close()
//
//	    ctx := context.Background()
//	    key := "client-ip-127.0.0.1"
//
//	    // Wait for the rate limiter to allow the event
//	    if err := kl.Wait(ctx, key); err != nil {
//	        panic(err)
//	    }
//
//	    // Or check immediately without blocking
//	    allowed, err := kl.Allow(key)
//	    if err != nil {
//	        panic(err)
//	    }
//	    fmt.Printf("Is action allowed? %t\n", allowed)
//	}
package limiter
