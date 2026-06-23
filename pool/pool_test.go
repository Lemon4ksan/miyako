// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPool_Basic(t *testing.T) {
	p := NewPool[int](Config{
		MinWorkers:  2,
		MaxWorkers:  5,
		IdleTimeout: 100 * time.Millisecond,
		QueueLimit:  10,
	})
	defer func() {
		_ = p.Close()
	}()

	ctx := context.Background()

	fut, err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, err := fut.Get(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}
}

func TestPool_ScaleUp(t *testing.T) {
	p := NewPool[string](Config{
		MinWorkers:  1,
		MaxWorkers:  3,
		IdleTimeout: 100 * time.Millisecond,
		QueueLimit:  10,
	})
	defer func() {
		_ = p.Close()
	}()

	ctx := context.Background()
	blockCh := make(chan struct{})

	defer close(blockCh)

	var wg sync.WaitGroup
	wg.Add(3)

	for range 3 {
		_, err := p.Submit(ctx, func(ctx context.Context) (string, error) {
			wg.Done()
			<-blockCh

			return "done", nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Wait until all 3 tasks are running
	wg.Wait()

	p.mu.Lock()
	workers := p.currentWorkers
	p.mu.Unlock()

	if workers != 3 {
		t.Errorf("expected pool to scale up to 3 workers, got %d", workers)
	}
}

func TestPool_ScaleDown(t *testing.T) {
	p := NewPool[string](Config{
		MinWorkers:  1,
		MaxWorkers:  3,
		IdleTimeout: 10 * time.Millisecond,
		QueueLimit:  10,
	})
	defer func() {
		_ = p.Close()
	}()

	ctx := context.Background()
	blockCh := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(3)

	for range 3 {
		_, err := p.Submit(ctx, func(ctx context.Context) (string, error) {
			wg.Done()
			<-blockCh

			return "done", nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	wg.Wait()
	close(blockCh) // release all tasks

	// Wait for idle timeout and worker scale down
	time.Sleep(30 * time.Millisecond)

	p.mu.Lock()
	workers := p.currentWorkers
	p.mu.Unlock()

	if workers != 1 {
		t.Errorf("expected pool to scale down to 1 worker, got %d", workers)
	}
}

func TestPool_Closed(t *testing.T) {
	p := NewPool[int](Config{
		MinWorkers: 1,
	})
	_ = p.Close()

	// Submitting to closed pool
	_, err := p.Submit(context.Background(), func(ctx context.Context) (int, error) {
		return 1, nil
	})
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}

	// Idempotent Close
	err = p.Close()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPool_QueueFull(t *testing.T) {
	p := NewPool[int](Config{
		MinWorkers: 1,
		MaxWorkers: 1,
		QueueLimit: 1,
	})
	defer func() {
		_ = p.Close()
	}()

	ctx := context.Background()
	blockCh := make(chan struct{})
	startCh := make(chan struct{})

	defer close(blockCh)

	// 1. Task that blocks worker
	_, err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		close(startCh)
		<-blockCh

		return 1, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for task 1 to start executing (meaning it has been popped from the queue)
	<-startCh

	// 2. Task that enters queue (length = 1)
	_, err = p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 3. Task that exceeds queue limit (returns ErrQueueFull)
	_, err = p.Submit(ctx, func(ctx context.Context) (int, error) {
		return 3, nil
	})
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("expected ErrQueueFull, got %v", err)
	}
}

func TestPool_PanicSafety(t *testing.T) {
	p := NewPool[int](Config{
		MinWorkers: 1,
	})
	defer func() {
		_ = p.Close()
	}()

	ctx := context.Background()

	fut, err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		panic("something went wrong")
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = fut.Get(ctx)
	if err == nil {
		t.Fatal("expected panic error, got nil")
	}

	expectedSub := "task panicked: something went wrong"
	if err.Error() != expectedSub {
		t.Errorf("expected %q, got %q", expectedSub, err.Error())
	}
}

func TestPool_FutureCancel(t *testing.T) {
	p := NewPool[int](Config{
		MinWorkers: 1,
	})
	defer func() {
		_ = p.Close()
	}()

	ctx := context.Background()
	blockCh := make(chan struct{})

	defer close(blockCh)

	fut, err := p.Submit(ctx, func(ctx context.Context) (int, error) {
		<-blockCh

		return 1, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel() // pre-canceled

	_, err = fut.Get(ctxCancel)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg := Config{}
	cfg.resolveDefaults()

	if cfg.MinWorkers != 1 {
		t.Errorf("expected MinWorkers to be 1, got %d", cfg.MinWorkers)
	}

	if cfg.MaxWorkers != 1 {
		t.Errorf("expected MaxWorkers to be 1, got %d", cfg.MaxWorkers)
	}

	if cfg.IdleTimeout != 5*time.Second {
		t.Errorf("expected IdleTimeout to be 5s, got %v", cfg.IdleTimeout)
	}

	if cfg.QueueLimit != 100 {
		t.Errorf("expected QueueLimit to be 100, got %d", cfg.QueueLimit)
	}

	cfg2 := Config{
		MinWorkers: 5,
		MaxWorkers: 2, // MaxWorkers < MinWorkers
	}
	cfg2.resolveDefaults()

	if cfg2.MaxWorkers != 5 {
		t.Errorf("expected MaxWorkers to be bumped to MinWorkers (5), got %d", cfg2.MaxWorkers)
	}
}
