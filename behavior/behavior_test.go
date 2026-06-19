// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package behavior

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockLogger struct {
	mu        sync.Mutex
	infoMsgs  []string
	errorMsgs []string
	warnMsgs  []string
}

func (l *mockLogger) Info(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.infoMsgs = append(l.infoMsgs, msg)
}

func (l *mockLogger) Error(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.errorMsgs = append(l.errorMsgs, msg)
}

func (l *mockLogger) Warn(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.warnMsgs = append(l.warnMsgs, msg)
}

type testBehavior struct {
	name    string
	runFunc func(ctx context.Context) error
	started atomic.Bool
}

func (t *testBehavior) Name() string { return t.name }

func (t *testBehavior) Run(ctx context.Context) error {
	t.started.Store(true)

	if t.runFunc != nil {
		return t.runFunc(ctx)
	}

	<-ctx.Done()

	return nil
}

func TestOrchestrator_BasicLifecycle(t *testing.T) {
	orch := NewOrchestrator()

	b1 := &testBehavior{name: "b1"}
	b2 := &testBehavior{name: "b2"}

	orch.Register(b1)
	orch.Register(b2)

	if orch.Count() != 2 {
		t.Fatalf("expected 2 behaviors, got %d", orch.Count())
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := orch.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if !b1.started.Load() {
		t.Error("b1 was not started")
	}

	if !b2.started.Load() {
		t.Error("b2 was not started")
	}

	orch.Stop()
	cancel()
}

func TestOrchestrator_DuplicateRegister(t *testing.T) {
	orch := NewOrchestrator()

	orch.Register(&testBehavior{name: "dup"})
	orch.Register(&testBehavior{name: "dup"})

	if orch.Count() != 1 {
		t.Fatalf("expected 1 behavior (duplicate skipped), got %d", orch.Count())
	}
}

func TestOrchestrator_AlreadyRunning(t *testing.T) {
	orch := NewOrchestrator()
	orch.Register(&testBehavior{name: "b1"})

	ctx := context.Background()

	if err := orch.Start(ctx); err != nil {
		t.Fatalf("first Start failed: %v", err)
	}

	defer orch.Stop()

	if err := orch.Start(ctx); err == nil {
		t.Fatal("expected error on second Start")
	}
}

func TestOrchestrator_FailFast(t *testing.T) {
	orch := NewOrchestrator(WithFailFast())

	errBehavior := &testBehavior{
		name: "failer",
		runFunc: func(ctx context.Context) error {
			return errors.New("boom")
		},
	}

	waiting := &testBehavior{name: "waiter"}

	orch.Register(errBehavior)
	orch.Register(waiting)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := orch.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for fail-fast to propagate
	time.Sleep(100 * time.Millisecond)

	orch.Stop()
}

func TestOrchestrator_StopWithoutStart(t *testing.T) {
	orch := NewOrchestrator()
	orch.Register(&testBehavior{name: "b1"})

	// Should not panic
	orch.Stop()
}

func TestOrchestrator_EmptyStop(t *testing.T) {
	orch := NewOrchestrator()

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	orch.Stop()
}

func TestOrchestrator_WithLogger(t *testing.T) {
	logger := &mockLogger{}
	orch := NewOrchestrator(WithLogger(logger))

	errBehavior := &testBehavior{
		name: "failer",
		runFunc: func(_ context.Context) error {
			return errors.New("boom")
		},
	}

	normalBehavior := &testBehavior{name: "normal"}

	orch.Register(errBehavior)
	orch.Register(normalBehavior)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := orch.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	orch.Stop()

	if len(logger.infoMsgs) == 0 {
		t.Fatal("expected at least one Info log")
	}

	if len(logger.errorMsgs) == 0 {
		t.Fatal("expected at least one Error log from failed behavior")
	}
}

func TestNopLogger(t *testing.T) {
	var l nopLogger

	l.Info("test info")
	l.Error("test error")
	l.Warn("test warn")
}
