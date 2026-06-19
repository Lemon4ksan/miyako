// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package jobs provides a concurrent-safe mechanism for tracking asynchronous
// request-response cycles by correlation ID.
//
// It is designed for building protocol implementations (TCP, UDP, WebSockets)
// where a request is sent with a unique ID and a response arrives later. The
// package handles job lifecycle including timeouts, context cancellation, and
// synchronous waiting.
//
// # Architecture
//
// The [Manager] stores pending jobs in a map keyed by correlation ID. Each job
// can have an optional timeout, context, callback, and keep-alive flag. When
// [Manager.Resolve] is called, the matching job's callback is invoked and the
// result is delivered to any goroutine blocked on [Manager.WaitFor].
//
// # Error Handling
//
// Jobs that time out or are cancelled via context receive the corresponding
// error through their callback or WaitFor return. [Manager.CancelAll] propagates
// a custom error to all pending jobs. The [Manager] uses a pluggable [Store]
// interface for the backing map - the default is an in-memory implementation.
//
// # Example - Callback Style
//
//	mgr := jobs.NewManager[string](100)
//	id := mgr.NextID()
//
//	err := mgr.Add(id, func(res string, err error) {
//	    if err != nil {
//	        log.Printf("Job %d failed: %v", id, err)
//	        return
//	    }
//	    fmt.Printf("Job %d received: %s", id, res)
//	})
//
//	// Somewhere else when response arrives:
//	mgr.Resolve(id, "Hello World", nil)
//
// # Example - Blocking Style
//
//	mgr := jobs.NewManager[string](0)
//	id := mgr.NextID()
//
//	mgr.Add(id, nil, jobs.WithWait[string](), jobs.WithTimeout[string](time.Second))
//
//	res, err := mgr.WaitFor(context.Background(), id)
//	if err != nil {
//	    log.Fatal(err)
//	}
package jobs
