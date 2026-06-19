// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// Scheduler coordinates task scheduling and prioritized execution.
// Initialize new instances using the [New] constructor before scheduling any tasks.
type Scheduler struct {
	mu       sync.Mutex
	tasks    taskHeap
	wakeChan chan struct{}
	taskPool sync.Pool
}

// New initializes and returns a new [Scheduler] instance.
// It configures internal task pooling and wake-up synchronization.
func New() *Scheduler {
	s := &Scheduler{
		wakeChan: make(chan struct{}, 1),
	}
	s.taskPool.New = func() any {
		return &Task{}
	}

	return s
}

// AcquireTask retrieves a clean [Task] instance from the internal pool.
// Always configure and schedule tasks returned by this method.
func (s *Scheduler) AcquireTask() *Task {
	return s.taskPool.Get().(*Task)
}

// ReleaseTask resets task fields and returns the [Task] back to the internal pool.
// Do not read or write to a task once it has been released.
func (s *Scheduler) ReleaseTask(t *Task) {
	t.ID = ""
	t.Execute = nil
	t.Interval = 0
	t.index = -1
	s.taskPool.Put(t)
}

// Schedule adds a [Task] to the prioritized execution queue.
// It automatically signals the active scheduler loop to reassess the queue.
func (s *Scheduler) Schedule(t *Task) {
	s.mu.Lock()
	heap.Push(&s.tasks, t)
	s.mu.Unlock()

	s.triggerWake()
}

func (s *Scheduler) triggerWake() {
	select {
	case s.wakeChan <- struct{}{}:
	default:
	}
}

// Start runs the scheduler loop, coordinating prioritized task execution.
// It blocks until the provided context is cancelled.
//
// Tasks are dispatched asynchronously in individual goroutines when their scheduled times arrive.
// It uses internal timers to sleep and wake up dynamically as the task queue changes.
func (s *Scheduler) Start(ctx context.Context) {
	var (
		timer     *time.Timer
		timerChan <-chan time.Time
	)

	for {
		s.mu.Lock()

		select {
		case <-ctx.Done():
			s.mu.Unlock()
			return
		default:
		}

		if len(s.tasks) == 0 {
			s.mu.Unlock()

			select {
			case <-ctx.Done():
				return
			case <-s.wakeChan:
				continue
			}
		}

		now := time.Now()
		nextTask := s.tasks[0]

		if now.Before(nextTask.NextRun) {
			delay := nextTask.NextRun.Sub(now)
			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				timer.Reset(delay)
			}

			timerChan = timer.C
			s.mu.Unlock()

			select {
			case <-ctx.Done():
				if timer != nil {
					timer.Stop()
				}

				return

			case <-timerChan:
				continue
			case <-s.wakeChan:
				continue
			}
		}

		task := heap.Pop(&s.tasks).(*Task)
		s.mu.Unlock()

		go s.runTask(ctx, task)
	}
}

func (s *Scheduler) runTask(ctx context.Context, t *Task) {
	if t.Execute != nil {
		_ = t.Execute(ctx)
	}

	if t.Interval > 0 {
		s.mu.Lock()
		t.NextRun = time.Now().Add(t.Interval)
		heap.Push(&s.tasks, t)
		s.mu.Unlock()
		s.triggerWake()
	} else {
		s.ReleaseTask(t)
	}
}
