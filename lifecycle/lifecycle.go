// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lifecycle

import (
	"context"
	"fmt"
	"sync"
)

// Service defines the lifecycle contract for a system module or background worker.
type Service interface {
	Name() string
	Init(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Dependent defines an optional interface for services that declare structural dependencies.
// Implementing this interface allows [Orchestrator] to perform topological sorting.
type Dependent interface {
	Service
	Dependencies() []string
}

// Orchestrator coordinates the initialization, startup, and graceful shutdown of registered services.
// It resolves dependencies topologically, ensuring services are started and stopped in the correct order.
// Initialize new instances using the [NewOrchestrator] constructor.
type Orchestrator struct {
	mu       sync.RWMutex
	services map[string]Service
	ordered  []Service
	running  []Service
	started  bool
}

// NewOrchestrator initializes and returns a new [Orchestrator] instance.
func NewOrchestrator() *Orchestrator {
	return &Orchestrator{
		services: make(map[string]Service),
	}
}

// All returns a slice containing all registered modules.
func (o *Orchestrator) All() []Service {
	o.mu.RLock()
	defer o.mu.RUnlock()

	res := make([]Service, 0, len(o.services))
	for _, mod := range o.services {
		res = append(res, mod)
	}

	return res
}

// Register adds a [Service] to the orchestrator.
func (o *Orchestrator) Register(s Service) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.services[s.Name()] = s
}

// InitAll performs topological sorting and initializes all registered services.
// It returns an error if circular dependencies are detected or if a service initialization fails.
func (o *Orchestrator) InitAll(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	ordered, err := o.sortDependencies()
	if err != nil {
		return fmt.Errorf("lifecycle: dependency resolution: %w", err)
	}

	o.ordered = ordered

	for _, s := range o.ordered {
		if err := s.Init(ctx); err != nil {
			return fmt.Errorf("lifecycle: init service %q failed: %w", s.Name(), err)
		}
	}

	return nil
}

// StartAll executes the startup routine for all initialized services in topological order.
// If starting any service fails, all previously started services are stopped immediately.
func (o *Orchestrator) StartAll(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.started {
		return nil
	}

	for _, s := range o.ordered {
		if err := s.Start(ctx); err != nil {
			o.stopRunning(context.Background())
			return fmt.Errorf("lifecycle: start service %q failed: %w", s.Name(), err)
		}

		o.running = append(o.running, s)
	}

	o.started = true

	return nil
}

// StopAll gracefully terminates all active services in reverse topological order.
func (o *Orchestrator) StopAll(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.stopRunning(ctx)
	o.started = false

	return nil
}

func (o *Orchestrator) stopRunning(ctx context.Context) {
	// Stop services in reverse order so that dependent modules are terminated
	// before their dependencies are taken offline.
	for i := len(o.running) - 1; i >= 0; i-- {
		s := o.running[i]
		_ = s.Stop(ctx)
	}

	o.running = nil
}

func (o *Orchestrator) sortDependencies() ([]Service, error) {
	visited := make(map[string]bool)
	temp := make(map[string]bool)

	var sorted []Service

	var visit func(name string) error

	visit = func(name string) error {
		if temp[name] {
			return fmt.Errorf("circular dependency detected involving %q", name)
		}

		if !visited[name] {
			temp[name] = true

			s, exists := o.services[name]
			if !exists {
				return fmt.Errorf("dependency %q is not registered", name)
			}

			if dep, ok := s.(Dependent); ok {
				for _, depName := range dep.Dependencies() {
					if err := visit(depName); err != nil {
						return err
					}
				}
			}

			temp[name] = false
			visited[name] = true

			sorted = append(sorted, s)
		}

		return nil
	}

	for name := range o.services {
		if !visited[name] {
			if err := visit(name); err != nil {
				return nil, err
			}
		}
	}

	return sorted, nil
}
