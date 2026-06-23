// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type mockService struct {
	name       string
	deps       []string
	initErr    error
	startErr   error
	stopErr    error
	initCalls  int
	startCalls int
	stopCalls  int
	mu         sync.Mutex
}

func (m *mockService) Name() string {
	return m.name
}

func (m *mockService) Init(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.initCalls++

	return m.initErr
}

func (m *mockService) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.startCalls++

	return m.startErr
}

func (m *mockService) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopCalls++

	return m.stopErr
}

func (m *mockService) Dependencies() []string {
	return m.deps
}

type mockSimpleService struct {
	name       string
	initCalls  int
	startCalls int
	stopCalls  int
}

func (m *mockSimpleService) Name() string {
	return m.name
}

func (m *mockSimpleService) Init(ctx context.Context) error {
	m.initCalls++
	return nil
}

func (m *mockSimpleService) Start(ctx context.Context) error {
	m.startCalls++
	return nil
}

func (m *mockSimpleService) Stop(ctx context.Context) error {
	m.stopCalls++
	return nil
}

func TestOrchestrator_HappyPath(t *testing.T) {
	o := NewOrchestrator()

	s1 := &mockService{name: "db"}
	s2 := &mockService{name: "cache", deps: []string{"db"}}
	s3 := &mockService{name: "api", deps: []string{"db", "cache"}}
	s4 := &mockSimpleService{name: "simple"} // Doesn't implement Dependent

	o.Register(s1)
	o.Register(s2)
	o.Register(s3)
	o.Register(s4)

	// Verify All contains all of them
	all := o.All()
	if len(all) != 4 {
		t.Fatalf("expected 4 registered services, got %d", len(all))
	}

	ctx := context.Background()

	// Init
	if err := o.InitAll(ctx); err != nil {
		t.Fatalf("unexpected error during InitAll: %v", err)
	}

	// Verify ordering: simple could be anywhere, but db must be before cache and api, cache must be before api
	// Let's inspect o.ordered directly
	var dbIdx, cacheIdx, apiIdx int
	for i, s := range o.ordered {
		switch s.Name() {
		case "db":
			dbIdx = i
		case "cache":
			cacheIdx = i
		case "api":
			apiIdx = i
		}
	}

	if dbIdx > cacheIdx || dbIdx > apiIdx || cacheIdx > apiIdx {
		t.Errorf("incorrect topological order: db index %d, cache index %d, api index %d", dbIdx, cacheIdx, apiIdx)
	}

	// Verify init was called on all
	if s1.initCalls != 1 || s2.initCalls != 1 || s3.initCalls != 1 || s4.initCalls != 1 {
		t.Errorf("expected each service to be initialized once")
	}

	// Start
	if err := o.StartAll(ctx); err != nil {
		t.Fatalf("unexpected error during StartAll: %v", err)
	}

	// StartAll again should be a no-op
	if err := o.StartAll(ctx); err != nil {
		t.Fatalf("unexpected error during second StartAll: %v", err)
	}

	if s1.startCalls != 1 || s2.startCalls != 1 || s3.startCalls != 1 || s4.startCalls != 1 {
		t.Errorf("expected each service to be started once")
	}

	// Stop
	if err := o.StopAll(ctx); err != nil {
		t.Fatalf("unexpected error during StopAll: %v", err)
	}

	if s1.stopCalls != 1 || s2.stopCalls != 1 || s3.stopCalls != 1 || s4.stopCalls != 1 {
		t.Errorf("expected each service to be stopped once")
	}
}

func TestOrchestrator_CircularDependency(t *testing.T) {
	o := NewOrchestrator()

	s1 := &mockService{name: "s1", deps: []string{"s2"}}
	s2 := &mockService{name: "s2", deps: []string{"s1"}}

	o.Register(s1)
	o.Register(s2)

	err := o.InitAll(context.Background())
	if err == nil {
		t.Fatal("expected error due to circular dependency, got nil")
	}
}

func TestOrchestrator_MissingDependency(t *testing.T) {
	o := NewOrchestrator()

	s1 := &mockService{name: "s1", deps: []string{"missing"}}

	o.Register(s1)

	err := o.InitAll(context.Background())
	if err == nil {
		t.Fatal("expected error due to missing dependency, got nil")
	}
}

func TestOrchestrator_InitFailure(t *testing.T) {
	o := NewOrchestrator()

	errDummy := errors.New("dummy init error")
	s1 := &mockService{name: "s1", initErr: errDummy}

	o.Register(s1)

	err := o.InitAll(context.Background())
	if !errors.Is(err, errDummy) {
		t.Fatalf("expected error %v, got %v", errDummy, err)
	}
}

func TestOrchestrator_StartFailure(t *testing.T) {
	o := NewOrchestrator()

	s1 := &mockService{name: "s1"}
	s2 := &mockService{name: "s2", deps: []string{"s1"}, startErr: errors.New("s2 start failed")}

	o.Register(s1)
	o.Register(s2)

	if err := o.InitAll(context.Background()); err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	err := o.StartAll(context.Background())
	if err == nil {
		t.Fatal("expected start error, got nil")
	}

	// Since s2 failed to start, s1 (which started successfully before s2) should have been stopped
	s1.mu.Lock()
	defer s1.mu.Unlock()

	if s1.startCalls != 1 {
		t.Errorf("expected s1 to be started, got %d calls", s1.startCalls)
	}

	if s1.stopCalls != 1 {
		t.Errorf("expected s1 to be stopped after s2 start failure, got %d calls", s1.stopCalls)
	}
}

func TestOrchestrator_StartBeforeInit(t *testing.T) {
	o := NewOrchestrator()
	s1 := &mockService{name: "s1"}
	o.Register(s1)

	// Since InitAll was never called, o.ordered is empty. StartAll starts 0 services.
	if err := o.StartAll(context.Background()); err != nil {
		t.Fatalf("unexpected start error: %v", err)
	}

	if s1.startCalls != 0 {
		t.Errorf("expected start to not be called on s1, got %d calls", s1.startCalls)
	}
}

func TestOrchestrator_ConcurrentRegister(t *testing.T) {
	o := NewOrchestrator()

	var wg sync.WaitGroup

	numGoroutines := 100

	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()

			o.Register(&mockService{name: string(rune(idx))})
			_ = o.All()
		}(i)
	}

	wg.Wait()
}
