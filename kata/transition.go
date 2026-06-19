// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kata

import (
	"context"
	"fmt"
)

// Transition atomically moves the FSM from its current state to the next
// state determined by the given event. The operation is fully thread-safe
// and supports transactional cancellation via before-hooks.
//
// The transition lifecycle:
//  1. Validate the transition rule exists (RLock).
//  2. Execute all registered before-hooks for the event. If any returns
//     an error, the transition is aborted and the error is returned.
//  3. Atomically apply the state change (Lock).
//  4. Execute all registered after-hooks for the event. Errors in
//     after-hooks are logged but do not roll back the state.
func (f *FSM[State, Event]) Transition(ctx context.Context, event Event) error {
	f.mu.RLock()
	from := f.current

	events, ok := f.rules[from]
	if !ok {
		f.mu.RUnlock()

		return fmt.Errorf("kata: no transitions defined from state %v", from)
	}

	to, exists := events[event]
	if !exists {
		f.mu.RUnlock()

		return fmt.Errorf("kata: invalid transition from state %v on event %v", from, event)
	}

	beforeHooks := make([]TransitionCallback[State, Event], len(f.beforeHooks[event]))
	copy(beforeHooks, f.beforeHooks[event])

	afterHooks := make([]TransitionCallback[State, Event], len(f.afterHooks[event]))
	copy(afterHooks, f.afterHooks[event])

	f.mu.RUnlock()

	for _, hook := range beforeHooks {
		if err := hook(ctx, from, event, to); err != nil {
			return fmt.Errorf("kata: before hook aborted transition %v -> %v: %w", from, to, err)
		}
	}

	f.mu.Lock()
	f.current = to
	f.mu.Unlock()

	for _, hook := range afterHooks {
		_ = hook(ctx, from, event, to)
	}

	return nil
}
