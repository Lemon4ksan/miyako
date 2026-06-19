// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package scheduler

import (
	"context"
	"sync"
	"time"
)

// Priority defines the execution precedence level of a [Task].
type Priority int

const (
	// PriorityLow represents the lowest task execution priority.
	PriorityLow Priority = iota
	// PriorityNormal represents the standard task execution priority.
	PriorityNormal
	// PriorityHigh represents the highest task execution priority.
	PriorityHigh
)

// Task represents a single schedulable unit of work.
// Acquire instances using [Scheduler.AcquireTask] and release them with [Scheduler.ReleaseTask].
type Task struct {
	// ID is the unique identifier of the task.
	ID string
	// Priority determines execution precedence when tasks are scheduled for the exact same time.
	Priority Priority
	// NextRun is the absolute time when the task is scheduled to execute next.
	NextRun time.Time
	// Interval is the duration between execution cycles for periodic tasks.
	// Set to <= 0 for one-off tasks.
	Interval time.Duration
	// Execute is the context-aware function executed by the scheduler.
	Execute func(ctx context.Context) error

	index int
}

// Debounce returns a thread-safe wrapped function that delays invoking fn until interval has elapsed.
// Each invocation of the returned function restarts the delay timer.
func Debounce(interval time.Duration, fn func()) func() {
	var (
		mu    sync.Mutex
		timer *time.Timer
	)

	return func() {
		mu.Lock()
		defer mu.Unlock()

		if timer != nil {
			timer.Stop()
		}

		timer = time.AfterFunc(interval, fn)
	}
}

// Throttle returns a thread-safe wrapped function that invokes fn at most once per interval.
// The first invocation runs asynchronously in a new goroutine, and subsequent calls are ignored during the cooldown.
func Throttle(interval time.Duration, fn func()) func() {
	var (
		mu      sync.Mutex
		lastRun time.Time
	)

	return func() {
		mu.Lock()
		defer mu.Unlock()

		now := time.Now()
		if now.Sub(lastRun) >= interval {
			lastRun = now

			go fn()
		}
	}
}

type taskHeap []*Task

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	if h[i].NextRun.Equal(h[j].NextRun) {
		return h[i].Priority > h[j].Priority
	}

	return h[i].NextRun.Before(h[j].NextRun)
}

func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *taskHeap) Push(x any) {
	n := len(*h)
	item := x.(*Task)
	item.index = n
	*h = append(*h, item)
}

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]

	return item
}
