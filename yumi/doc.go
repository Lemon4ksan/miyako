// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package yumi provides a generic concurrent pipeline for bulk data processing.
//
// It parallelizes slice or stream transformations with configurable worker
// count, rate limiting, input-order preservation, and optional fail-fast
// behavior. Object pooling via [sync.Pool] minimizes allocation overhead.
//
// # Architecture
//
// The [Pipeline] distributes input items across a fixed pool of worker
// goroutines via a buffered channel. Each worker pulls items, applies the
// mapper function, and sends results to an output channel. Results are
// reassembled in input order using index tracking. A [sync.Pool] recycles
// task objects to reduce GC pressure.
//
// # Error Handling
//
// In default mode, all errors are collected and returned via [errors.Join].
// With [PipelineConfig.FailFast] enabled, the first error cancels the context
// for all workers and is returned immediately. Context cancellation propagates
// to all workers for clean shutdown.
//
// # Example - One-liner
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//	    "strings"
//
//	    "github.com/lemon4ksan/miyako/yumi"
//	)
//
//	func main() {
//	    inputs := []string{"hello", "world", "foo", "bar"}
//
//	    results, err := yumi.Map(context.Background(), yumi.PipelineConfig{
//	        Workers: 2,
//	    }, inputs, func(ctx context.Context, s string) (string, error) {
//	        return strings.ToUpper(s), nil
//	    })
//
//	    if err != nil {
//	        panic(err)
//	    }
//
//	    fmt.Println(results) // [HELLO WORLD FOO BAR]
//	}
//
// # Example - Streaming
//
//	p := yumi.NewPipeline[string, string](yumi.PipelineConfig{Workers: 4})
//
//	out, errs := p.Stream(ctx, inputChan, func(ctx context.Context, s string) (string, error) {
//	    return transform(s), nil
//	})
//
//	for v := range out {
//	    fmt.Println(v)
//	}
package yumi
