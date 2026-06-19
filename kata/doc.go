// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package kata implements a strictly typed, thread-safe finite state machine.
//
// It provides a generic FSM framework parameterized over comparable [State] and
// [Event] types, catching invalid transitions at compile time. All state changes
// are atomic and thread-safe, supporting concurrent goroutines. Before/after
// hooks enable transactional rollback and side effects.
//
// # Architecture
//
// The [FSM] stores transition rules in a two-level map: State → Event → State,
// providing O(1) lookup. [Transition] acquires an RLock to validate the rule
// and copy hooks, releases it, executes before-hooks, then acquires a write
// lock to atomically apply the state change. This avoids holding the write lock
// during user-provided callbacks.
//
// # Error Handling
//
// If a before-hook returns an error, the transition is aborted and the state
// remains unchanged. After-hook errors are ignored since the state has already
// changed. Invalid transitions (no matching rule) return an error without
// modifying state.
//
// # Example
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//
//	    "github.com/lemon4ksan/miyako/kata"
//	)
//
//	type State int
//	const (
//	    Idle State = iota
//	    Running
//	    Stopped
//	)
//
//	type Event int
//	const (
//	    Start Event = iota
//	    Stop
//	)
//
//	func main() {
//	    fsm := kata.NewFSM[State, Event](Idle)
//
//	    fsm.AddRules(
//	        kata.TransitionRule[State, Event]{From: Idle, Event: Start, To: Running},
//	        kata.TransitionRule[State, Event]{From: Running, Event: Stop, To: Stopped},
//	    )
//
//	    fsm.OnBefore(Start, func(ctx context.Context, from State, event Event, to State) error {
//	        fmt.Printf("Transitioning: %v -> %v\n", from, to)
//	        return nil
//	    })
//
//	    if err := fsm.Transition(context.Background(), Start); err != nil {
//	        panic(err)
//	    }
//
//	    fmt.Println(fsm.ToDOT())
//	}
package kata
