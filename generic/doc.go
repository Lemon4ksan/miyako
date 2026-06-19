// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package generic provides a lightweight, high-performance, and type-safe
// utility toolkit for Go. It leverages Go generics to eliminate repetitive
// boilerplate code, manual type assertions, and unsafe reflection on hot paths.
//
// The package is strictly divided into four cohesive areas of responsibility:
//   - Basic Primitives (types.go): Fluent pointer manipulation, null-coalescing, and ternary operations.
//   - Slice Operations (slices.go): High-performance slicing, grouping, chunking, and in-place filtering.
//   - Custom Collections (collections.go): Thread-safe sets and TTL-based in-memory caches.
//   - Advanced Concurrency (concurrency.go): Parallel maps, asynchronous Futures, SingleFlight de-duplicators,
//     and thread-safe jittered backoffs.
//
// # Design Principles
//
//   - Generics-First: Every function is parameterized, guaranteeing compile-time type safety.
//   - Zero-Dependency: The package relies strictly on the Go standard library, keeping its footprint minimal.
//   - Performance & Memory Safety: Concurrency primitives are thread-safe, and slice helpers are optimized
//     to reduce allocations on the heap (e.g., using sync.Pool, capacity pre-allocation, or in-place memory reuse).
//
// # Basic Primitives & Pointers
//
// In Go, taking the address of literals directly (e.g. &100) is invalid. The [Ptr] helper resolves
// this cleanly, allowing inline pointer construction. Additionally, [Deref] and [DerefOr] provide
// safe dereferencing of pointers without nil-pointer panics:
//
//	type Config struct {
//		Limit *int
//	}
//
//	cfg := Config{
//		Limit: generic.Ptr(100), // Clean literal pointer
//	}
//
//	val := generic.DerefOr(cfg.Limit, 10) // Safely falls back to 10 if nil
//
// # High-Performance Slice Operations
//
// Manipulating slices in Go often requires writing repetitive loop structures.
// This package provides functional helpers like [IndexBy] and [FilterInPlace]:
//
//	items := []string{"foo", "bar", "baz"}
//
//	// Index slice into a map with O(1) lookups
//	indexed := generic.IndexBy(items, func(s string) string { return s[:1] }) // map[f:foo b:bar]
//
//	// Chunk slices into batches of size N
//	batches := generic.Chunk(items, 2) // [][]string{{"foo", "bar"}, {"baz"}}
//
// # Advanced Concurrency & Resilience
//
// Writing stable, multi-threaded pipelines in Go requires careful synchronization. This package provides
// highly optimized concurrency primitives:
//   - [ParallelMap]: Concurrently transforms a slice with a strict worker pool limit.
//   - [ParallelForEach]: Concurrently runs side-effect tasks with semaphore bounds and error aggregation.
//   - [Future]: Simple, thread-safe async-execution wrapper (Promise pattern).
//   - [SingleFlight]: Thread-safe task suppressor that merges concurrent calls to prevent backend spam.
//   - [Backoff]: Thread-safe exponential backoff with randomized jitter (AWS algorithm).
//
// # Concurrency Example
//
//	package main
//
//	import (
//		"context"
//		"fmt"
//		"time"
//		"github.com/lemon4ksan/generic"
//	)
//
//	func main() {
//		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//		defer cancel()
//
//		inputs := []string{"item1", "item2", "item3"}
//
//		// Process items concurrently, limiting to 2 parallel goroutines
//		results := generic.ParallelMap(ctx, inputs, 2, func(c context.Context, item string) string {
//			return item + "_processed"
//		})
//
//		fmt.Println("Processed results:", results)
//	}
package generic
