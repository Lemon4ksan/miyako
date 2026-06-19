// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kata

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type testState int

const (
	stateA testState = iota
	stateB
	stateC
)

type testEvent int

const (
	eventGo testEvent = iota
	eventBack
	eventReset
)

func TestFSM_BasicTransition(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
		TransitionRule[testState, testEvent]{From: stateB, Event: eventGo, To: stateC},
		TransitionRule[testState, testEvent]{From: stateB, Event: eventBack, To: stateA},
		TransitionRule[testState, testEvent]{From: stateC, Event: eventReset, To: stateA},
	)

	if got := fsm.CurrentState(); got != stateA {
		t.Fatalf("expected initial state %v, got %v", stateA, got)
	}

	if err := fsm.Transition(context.Background(), eventGo); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := fsm.CurrentState(); got != stateB {
		t.Fatalf("expected state %v after transition, got %v", stateB, got)
	}

	if err := fsm.Transition(context.Background(), eventGo); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := fsm.CurrentState(); got != stateC {
		t.Fatalf("expected state %v after second transition, got %v", stateC, got)
	}

	if err := fsm.Transition(context.Background(), eventReset); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := fsm.CurrentState(); got != stateA {
		t.Fatalf("expected state %v after reset, got %v", stateA, got)
	}
}

func TestFSM_InvalidTransition(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
	)

	err := fsm.Transition(context.Background(), eventBack)
	if err == nil {
		t.Fatal("expected error for invalid transition, got nil")
	}

	if !strings.Contains(err.Error(), "invalid transition") {
		t.Fatalf("expected 'invalid transition' in error, got: %v", err)
	}
}

func TestFSM_BeforeHookRollback(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
	)

	abortErr := errors.New("aborting transition")

	fsm.OnBefore(eventGo, func(_ context.Context, _ testState, _ testEvent, _ testState) error {
		return abortErr
	})

	err := fsm.Transition(context.Background(), eventGo)
	if err == nil {
		t.Fatal("expected error from before-hook, got nil")
	}

	if !errors.Is(err, abortErr) {
		t.Fatalf("expected wrapped abort error, got: %v", err)
	}

	if got := fsm.CurrentState(); got != stateA {
		t.Fatalf("state should remain %v after rollback, got %v", stateA, got)
	}
}

func TestFSM_BeforeAndAfterHooks(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
	)

	var beforeCalled, afterCalled atomic.Bool

	fsm.OnBefore(eventGo, func(_ context.Context, from testState, event testEvent, to testState) error {
		if from != stateA || to != stateB || event != eventGo {
			t.Errorf("before hook received wrong args: from=%v, event=%v, to=%v", from, event, to)
		}

		beforeCalled.Store(true)

		return nil
	})

	fsm.OnAfter(eventGo, func(_ context.Context, from testState, event testEvent, to testState) error {
		if from != stateA || to != stateB || event != eventGo {
			t.Errorf("after hook received wrong args: from=%v, event=%v, to=%v", from, event, to)
		}

		afterCalled.Store(true)

		return nil
	})

	if err := fsm.Transition(context.Background(), eventGo); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !beforeCalled.Load() {
		t.Fatal("before hook was not called")
	}

	if !afterCalled.Load() {
		t.Fatal("after hook was not called")
	}

	if got := fsm.CurrentState(); got != stateB {
		t.Fatalf("expected state %v, got %v", stateB, got)
	}
}

func TestFSM_AfterHookErrorIgnored(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
	)

	fsm.OnAfter(eventGo, func(_ context.Context, _ testState, _ testEvent, _ testState) error {
		return errors.New("after hook error")
	})

	err := fsm.Transition(context.Background(), eventGo)
	if err != nil {
		t.Fatalf("transition should succeed even if after-hook fails, got: %v", err)
	}

	if got := fsm.CurrentState(); got != stateB {
		t.Fatalf("expected state %v, got %v", stateB, got)
	}
}

func TestFSM_MultipleBeforeHooksOrder(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
	)

	var order []int

	fsm.OnBefore(eventGo, func(_ context.Context, _ testState, _ testEvent, _ testState) error {
		order = append(order, 1)

		return nil
	})

	fsm.OnBefore(eventGo, func(_ context.Context, _ testState, _ testEvent, _ testState) error {
		order = append(order, 2)

		return nil
	})

	if err := fsm.Transition(context.Background(), eventGo); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("expected hooks called in order [1, 2], got %v", order)
	}
}

func TestFSM_ConcurrentTransitions(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
		TransitionRule[testState, testEvent]{From: stateB, Event: eventBack, To: stateA},
	)

	const goroutines = 100

	var (
		wg        sync.WaitGroup
		succCount atomic.Int64
		errCount  atomic.Int64
	)

	for range goroutines {
		wg.Add(1)

		go func() {
			defer wg.Done()

			err := fsm.Transition(context.Background(), eventGo)
			if err != nil {
				errCount.Add(1)
			} else {
				succCount.Add(1)
			}
		}()
	}

	wg.Wait()

	if succCount.Load() != 1 {
		t.Fatalf("expected exactly 1 successful transition, got %d", succCount.Load())
	}

	if errCount.Load() != goroutines-1 {
		t.Fatalf("expected %d errors, got %d", goroutines-1, errCount.Load())
	}

	state := fsm.CurrentState()
	if state != stateB {
		t.Fatalf("expected final state %v, got %v", stateB, state)
	}
}

func TestFSM_ConcurrentTransitionsRace(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
		TransitionRule[testState, testEvent]{From: stateB, Event: eventBack, To: stateA},
	)

	const rounds = 50

	var wg sync.WaitGroup

	for range rounds {
		wg.Add(2)

		go func() {
			defer wg.Done()

			_ = fsm.Transition(context.Background(), eventGo)
		}()

		go func() {
			defer wg.Done()

			_ = fsm.Transition(context.Background(), eventBack)
		}()
	}

	wg.Wait()

	state := fsm.CurrentState()
	if state != stateA && state != stateB {
		t.Fatalf("unexpected final state after race: %v", state)
	}
}

func TestFSM_VerifyStateAfterAlternatingTransitions(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
		TransitionRule[testState, testEvent]{From: stateB, Event: eventBack, To: stateA},
	)

	for range 100 {
		if err := fsm.Transition(context.Background(), eventGo); err != nil {
			t.Fatalf("transition A->B failed: %v", err)
		}

		if got := fsm.CurrentState(); got != stateB {
			t.Fatalf("expected state %v, got %v", stateB, got)
		}

		if err := fsm.Transition(context.Background(), eventBack); err != nil {
			t.Fatalf("transition B->A failed: %v", err)
		}

		if got := fsm.CurrentState(); got != stateA {
			t.Fatalf("expected state %v, got %v", stateA, got)
		}
	}
}

func TestFSM_Validate(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
	)

	to, ok := fsm.Validate(eventGo)
	if !ok || to != stateB {
		t.Fatalf("expected valid transition to %v, got %v (ok=%v)", stateB, to, ok)
	}

	_, ok = fsm.Validate(eventBack)
	if ok {
		t.Fatal("expected invalid transition for eventBack from stateA")
	}
}

func TestFSM_Validate_NoRulesForState(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateC)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
	)

	_, ok := fsm.Validate(eventGo)
	if ok {
		t.Fatal("expected invalid transition when no rules exist for current state")
	}
}

func TestFSM_ToDOT(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
		TransitionRule[testState, testEvent]{From: stateB, Event: eventGo, To: stateC},
	)

	dot := fsm.ToDOT()

	if !strings.Contains(dot, "digraph FSM") {
		t.Fatal("DOT output missing digraph header")
	}

	if !strings.Contains(dot, `"0"`) {
		t.Fatal("DOT output missing stateA (0) node")
	}

	if !strings.Contains(dot, `"1"`) {
		t.Fatal("DOT output missing stateB (1) node")
	}

	if !strings.Contains(dot, `"2"`) {
		t.Fatal("DOT output missing stateC (2) node")
	}

	if !strings.Contains(dot, "[label=\"0\"]") {
		t.Fatal("DOT output missing eventGo transition label")
	}
}

func TestFSM_CurrentState_Concurrent(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
	)

	var wg sync.WaitGroup

	const readers = 50

	for range readers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_ = fsm.CurrentState()
		}()
	}

	wg.Wait()
}

func TestFSM_String(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
	)

	s := fsm.String()
	if !strings.Contains(s, "current=0") {
		t.Fatalf("String() should contain current state, got: %s", s)
	}
}

func TestFSM_ContextCancellation(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fsm.OnBefore(eventGo, func(ctx context.Context, _ testState, _ testEvent, _ testState) error {
		return ctx.Err()
	})

	err := fsm.Transition(ctx, eventGo)
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}

	if got := fsm.CurrentState(); got != stateA {
		t.Fatalf("state should remain %v after cancelled before-hook, got %v", stateA, got)
	}
}

func TestFSM_NoTransitionsDefined(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	err := fsm.Transition(context.Background(), eventGo)
	if err == nil {
		t.Fatal("expected error when no rules are defined, got nil")
	}

	if !strings.Contains(err.Error(), "no transitions defined") {
		t.Fatalf("expected 'no transitions defined' in error, got: %v", err)
	}
}

func TestFSM_ForceSet(t *testing.T) {
	fsm := NewFSM[testState, testEvent](stateA)

	fsm.AddRules(
		TransitionRule[testState, testEvent]{From: stateA, Event: eventGo, To: stateB},
		TransitionRule[testState, testEvent]{From: stateC, Event: eventReset, To: stateA},
	)

	fsm.ForceSet(stateC)

	if got := fsm.CurrentState(); got != stateC {
		t.Fatalf("expected state %v after ForceSet, got %v", stateC, got)
	}

	if err := fsm.Transition(context.Background(), eventReset); err != nil {
		t.Fatalf("transition from forced state should work: %v", err)
	}

	if got := fsm.CurrentState(); got != stateA {
		t.Fatalf("expected state %v after transition, got %v", stateA, got)
	}
}
