// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package scheduler

import (
	"container/heap"
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestScheduler_Start_PeriodicExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sched := New()
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		var executedCount atomic.Int32

		task := sched.AcquireTask()
		task.ID = "periodic-task"
		task.Priority = PriorityNormal
		task.Interval = 2 * time.Hour
		task.NextRun = time.Now().Add(2 * time.Hour)
		task.Execute = func(ctx context.Context) error {
			executedCount.Add(1)
			return nil
		}

		sched.Schedule(task)

		go sched.Start(ctx)

		time.Sleep(5 * time.Hour)

		cancel()
		synctest.Wait()

		got := executedCount.Load()
		if got != 2 {
			t.Errorf("executed count = %d, want %d", got, 2)
		}
	})
}

func TestScheduler_Start_OneOffExecution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sched := New()
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		var executedCount atomic.Int32

		task := sched.AcquireTask()
		task.ID = "one-off-task"
		task.Priority = PriorityNormal
		task.Interval = 0
		task.NextRun = time.Now().Add(1 * time.Hour)
		task.Execute = func(ctx context.Context) error {
			executedCount.Add(1)
			return nil
		}

		sched.Schedule(task)

		go sched.Start(ctx)

		time.Sleep(5 * time.Hour)

		cancel()
		synctest.Wait()

		got := executedCount.Load()
		if got != 1 {
			t.Errorf("executed count = %d, want %d", got, 1)
		}
	})
}

func TestScheduler_Start_ChronologicalOrdering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sched := New()
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		var (
			mu            sync.Mutex
			executedOrder []string
		)

		t1 := sched.AcquireTask()
		t1.ID = "first"
		t1.Priority = PriorityNormal
		t1.NextRun = time.Now().Add(1 * time.Hour)
		t1.Execute = func(ctx context.Context) error {
			mu.Lock()

			executedOrder = append(executedOrder, "first")
			mu.Unlock()

			return nil
		}

		t2 := sched.AcquireTask()
		t2.ID = "second"
		t2.Priority = PriorityNormal
		t2.NextRun = time.Now().Add(2 * time.Hour)
		t2.Execute = func(ctx context.Context) error {
			mu.Lock()

			executedOrder = append(executedOrder, "second")
			mu.Unlock()

			return nil
		}

		sched.Schedule(t2)
		sched.Schedule(t1)

		go sched.Start(ctx)

		// Advance time step-by-step to hit chronological checkpoints
		time.Sleep(150 * time.Minute)

		cancel()
		synctest.Wait()

		mu.Lock()
		defer mu.Unlock()

		want := []string{"first", "second"}
		if !slices.Equal(executedOrder, want) {
			t.Errorf("executed order = %v, want %v", executedOrder, want)
		}
	})
}

func TestTaskHeap_PriorityOrdering(t *testing.T) {
	targetTime := time.Now()
	h := &taskHeap{}

	tLow := &Task{ID: "low", Priority: PriorityLow, NextRun: targetTime}
	tNormal := &Task{ID: "normal", Priority: PriorityNormal, NextRun: targetTime}
	tHigh := &Task{ID: "high", Priority: PriorityHigh, NextRun: targetTime}

	heap.Push(h, tLow)
	heap.Push(h, tNormal)
	heap.Push(h, tHigh)

	p1 := heap.Pop(h).(*Task)
	p2 := heap.Pop(h).(*Task)
	p3 := heap.Pop(h).(*Task)

	got := []string{p1.ID, p2.ID, p3.ID}
	want := []string{"high", "normal", "low"}

	if !slices.Equal(got, want) {
		t.Errorf("heap pop order = %v, want %v", got, want)
	}
}

func TestDebounce_Execution_DelaysAndResets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var executedCount atomic.Int32

		debounced := Debounce(100*time.Millisecond, func() {
			executedCount.Add(1)
		})

		debounced()
		time.Sleep(50 * time.Millisecond)
		debounced()
		time.Sleep(50 * time.Millisecond)

		if executedCount.Load() != 0 {
			t.Errorf("executed count before timeout = %d, want 0", executedCount.Load())
		}

		time.Sleep(100 * time.Millisecond)

		got := executedCount.Load()
		if got != 1 {
			t.Errorf("executed count after timeout = %d, want 1", got)
		}
	})
}

func TestThrottle_Execution_LimitsRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var executedCount atomic.Int32

		throttled := Throttle(100*time.Millisecond, func() {
			executedCount.Add(1)
		})

		throttled()
		throttled()
		throttled()

		synctest.Wait()

		if executedCount.Load() != 1 {
			t.Errorf("executed count immediately = %d, want 1", executedCount.Load())
		}

		time.Sleep(150 * time.Millisecond)

		throttled()
		synctest.Wait()

		got := executedCount.Load()
		if got != 2 {
			t.Errorf("executed count after cooldown = %d, want 2", got)
		}
	})
}

func TestScheduler_Start_WakeChanDuringTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sched := New()
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		var (
			executedList []string
			mu           sync.Mutex
		)

		// Task A: far in the future
		taskA := sched.AcquireTask()
		taskA.ID = "A"
		taskA.NextRun = time.Now().Add(10 * time.Hour)
		taskA.Execute = func(ctx context.Context) error {
			mu.Lock()

			executedList = append(executedList, "A")
			mu.Unlock()

			return nil
		}
		sched.Schedule(taskA)

		// Start scheduler - it will sleep waiting for task A
		go sched.Start(ctx)

		// Sleep 1 hour, scheduler is still waiting for task A (9 hours left)
		time.Sleep(1 * time.Hour)

		// Schedule Task B: closer in the future (at +2 hours relative to original now, i.e., +1 hour from current now)
		taskB := sched.AcquireTask()
		taskB.ID = "B"
		taskB.NextRun = time.Now().Add(1 * time.Hour)
		taskB.Execute = func(ctx context.Context) error {
			mu.Lock()

			executedList = append(executedList, "B")
			mu.Unlock()

			return nil
		}
		// Scheduling B triggers wakeChan inside timer select
		sched.Schedule(taskB)

		// Sleep 2 more hours to allow Task B to execute
		time.Sleep(2 * time.Hour)

		cancel()
		synctest.Wait()

		mu.Lock()
		defer mu.Unlock()

		if len(executedList) != 1 || executedList[0] != "B" {
			t.Errorf("expected B to execute, executedList = %v", executedList)
		}
	})
}

func TestScheduler_Start_AlreadyCanceled(t *testing.T) {
	sched := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Already canceled

	// This should return immediately without blocking
	sched.Start(ctx)
}
