// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package behavior

import (
	"context"
	"errors"
	"sync"
)

// Behavior represents a modular task that the orchestrator can run.
type Behavior interface {
	// Name returns the unique name of the behavior.
	Name() string
	// Run starts the behavior's main loop. It should block until the context
	// is canceled or an unrecoverable error occurs.
	Run(ctx context.Context) error
}

// Logger defines the interface for behavior logging.
// Any type implementing this interface can be used with the orchestrator.
type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
}

// nopLogger is a no-op logger for when no logger is configured.
type nopLogger struct{}

func (nopLogger) Info(_ string, _ ...any)  {}
func (nopLogger) Error(_ string, _ ...any) {}
func (nopLogger) Warn(_ string, _ ...any)  {}

// Option configures the orchestrator.
type Option func(*Orchestrator)

// WithLogger sets the logger for the orchestrator.
func WithLogger(l Logger) Option {
	return func(o *Orchestrator) {
		o.logger = l
	}
}

// WithFailFast enables fail-fast mode: if any behavior returns an error,
// all other behaviors are canceled immediately.
func WithFailFast() Option {
	return func(o *Orchestrator) {
		o.failFast = true
	}
}

// Orchestrator manages the lifecycle of multiple behaviors.
type Orchestrator struct {
	logger    Logger
	behaviors []Behavior
	mu        sync.RWMutex
	running   bool
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	failFast  bool
}

// NewOrchestrator creates a new orchestrator with the given options.
func NewOrchestrator(opts ...Option) *Orchestrator {
	o := &Orchestrator{
		logger:    nopLogger{},
		behaviors: make([]Behavior, 0),
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// Register adds a behavior to the orchestrator.
// If a behavior with the same name is already registered, it is skipped.
func (o *Orchestrator) Register(b Behavior) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for _, existing := range o.behaviors {
		if existing.Name() == b.Name() {
			o.logger.Warn("Behavior already registered, skipping", "name", b.Name())
			return
		}
	}

	o.behaviors = append(o.behaviors, b)
}

// Start starts all registered behaviors in separate goroutines.
// It returns an error if the orchestrator is already running.
func (o *Orchestrator) Start(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.running {
		return errors.New("orchestrator is already running")
	}

	o.running = true
	runCtx, cancel := context.WithCancel(ctx)
	o.cancel = cancel

	for _, b := range o.behaviors {
		o.wg.Add(1)

		go func(beh Behavior) {
			defer o.wg.Done()

			o.logger.Info("Starting behavior", "name", beh.Name())

			if err := beh.Run(runCtx); err != nil {
				if runCtx.Err() == nil {
					o.logger.Error("Behavior failed", "name", beh.Name(), "error", err)

					if o.failFast {
						cancel()
					}
				}
			} else {
				o.logger.Info("Behavior stopped", "name", beh.Name())
			}
		}(b)
	}

	return nil
}

// Stop stops all running behaviors and waits for them to finish.
func (o *Orchestrator) Stop() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if !o.running {
		return
	}

	o.cancel()
	o.wg.Wait()
	o.running = false
}

// Count returns the number of registered behaviors.
func (o *Orchestrator) Count() int {
	o.mu.RLock()
	defer o.mu.RUnlock()

	return len(o.behaviors)
}
