// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bus implements a type-based event bus for asynchronous in-process
// communication.
//
// It provides a decoupled architecture where components can publish and subscribe
// to typed events without direct dependencies. Events are routed by their
// [reflect.Type], so pointer and value types of the same struct resolve to the
// same channel.
//
// # Architecture
//
// The [Bus] holds two maps: one for type-specific subscriptions and one for
// "subscribe-all" subscriptions. [Publish] is non-blocking - if a subscriber's
// channel buffer is full, the event is silently dropped to prevent publisher
// backpressure. All operations are protected by [sync.RWMutex] for thread safety.
//
// # Error Handling
//
// The bus is designed for fire-and-forget event distribution. There is no
// error return from [Publish] because events that cannot be delivered (full
// buffer, closed bus) are silently dropped. If you need guaranteed delivery,
// use [jobs.Manager] instead.
//
// # Example
//
//	package main
//
//	import (
//	    "fmt"
//	    "sync"
//
//	    "github.com/lemon4ksan/miyako/bus"
//	)
//
//	type MyEvent struct {
//	    bus.BaseEvent
//	    Message string
//	}
//
//	func main() {
//	    b := bus.New()
//	    sub := b.Subscribe(MyEvent{})
//	    defer sub.Unsubscribe()
//
//	    var wg sync.WaitGroup
//
//	    wg.Go(func() {
//	        for ev := range sub.C() {
//	            msg := ev.(MyEvent).Message
//	            fmt.Println("Received:", msg)
//	        }
//	    })
//
//	    b.Publish(MyEvent{Message: "Hello!"})
//	    b.Close()
//	    wg.Wait()
//	}
package bus
