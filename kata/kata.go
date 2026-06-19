// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kata

import (
	"context"
	"fmt"
	"sync"
)

// TransitionCallback is the function signature for transition hooks.
// It receives the context, source state, triggering event, and target state.
type TransitionCallback[State comparable, Event comparable] func(
	ctx context.Context,
	from State,
	event Event,
	to State,
) error

// TransitionRule defines a valid state transition triggered by an event.
type TransitionRule[State comparable, Event comparable] struct {
	From  State
	Event Event
	To    State
}

// FSM is a strictly typed, thread-safe finite state machine parameterized
// over State and Event comparable types. Create instances via [NewFSM].
type FSM[State comparable, Event comparable] struct {
	mu          sync.RWMutex
	current     State
	rules       map[State]map[Event]State
	beforeHooks map[Event][]TransitionCallback[State, Event]
	afterHooks  map[Event][]TransitionCallback[State, Event]
}

// NewFSM creates a new finite state machine with the given initial state.
func NewFSM[State, Event comparable](initial State) *FSM[State, Event] {
	return &FSM[State, Event]{
		current:     initial,
		rules:       make(map[State]map[Event]State),
		beforeHooks: make(map[Event][]TransitionCallback[State, Event]),
		afterHooks:  make(map[Event][]TransitionCallback[State, Event]),
	}
}

// AddRules registers one or more valid transition rules in the state machine.
// Duplicate rules for the same (From, Event) pair are overwritten by the last entry.
func (f *FSM[State, Event]) AddRules(rules ...TransitionRule[State, Event]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, r := range rules {
		if f.rules[r.From] == nil {
			f.rules[r.From] = make(map[Event]State)
		}

		f.rules[r.From][r.Event] = r.To
	}
}

// CurrentState returns the current state of the FSM.
func (f *FSM[State, Event]) CurrentState() State {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.current
}

// ForceSet bypasses the transition rules and sets the current state directly.
// This is intended for test setup where you need to place the FSM into a
// specific precondition state without going through valid transitions.
// Using ForceSet in production code is strongly discouraged.
func (f *FSM[State, Event]) ForceSet(state State) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.current = state
}

// OnBefore registers a callback that is invoked before a transition
// for the given event is applied. If any callback returns an error,
// the transition is cancelled and the state remains unchanged.
func (f *FSM[State, Event]) OnBefore(event Event, cb TransitionCallback[State, Event]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.beforeHooks[event] = append(f.beforeHooks[event], cb)
}

// OnAfter registers a callback that is invoked after a transition
// for the given event has been applied. Errors in after-hooks are
// ignored since the state has already changed.
func (f *FSM[State, Event]) OnAfter(event Event, cb TransitionCallback[State, Event]) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.afterHooks[event] = append(f.afterHooks[event], cb)
}

// Validate checks whether a transition from the current state on the
// given event is defined. It returns the target state and true if valid,
// or the zero value and false if no such rule exists.
func (f *FSM[State, Event]) Validate(event Event) (State, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	events, ok := f.rules[f.current]
	if !ok {
		var zero State

		return zero, false
	}

	to, ok := events[event]

	return to, ok
}

// String returns a human-readable representation of the FSM for debugging.
func (f *FSM[State, Event]) String() string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return fmt.Sprintf("FSM{current=%v, rules=%d}", f.current, f.countRules())
}

func (f *FSM[State, Event]) countRules() int {
	n := 0

	for _, events := range f.rules {
		n += len(events)
	}

	return n
}
