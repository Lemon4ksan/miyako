// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package lifecycle manages dependency-aware application startup and graceful
// shutdown.
//
// It organizes registered services into a directed acyclic graph (DAG) based on
// declared dependencies, executing their initialization, startup, and
// termination phases in topological order. On failure during StartAll, already
// started services are automatically rolled back in reverse order.
//
// # Architecture
//
// The [Orchestrator] performs a DFS-based topological sort of all registered
// [Service] instances that implement [Dependent]. The sort order determines the
// Init and Start sequences; Stop runs in the exact reverse. Circular dependencies
// are detected and reported as errors before any service is initialized.
//
// # Error Handling
//
// If [Orchestrator.InitAll] fails, no services are started and no rollback is
// needed. If [Orchestrator.StartAll] fails, all successfully started services
// are stopped in reverse order (rollback). [Orchestrator.StopAll] is idempotent
// and can be called multiple times safely.
//
// # Example
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//	    "time"
//
//	    "github.com/lemon4ksan/miyako/lifecycle"
//	)
//
//	type DatabaseService struct{}
//	func (d *DatabaseService) Name() string            { return "db" }
//	func (d *DatabaseService) Init(ctx context.Context) error  { return nil }
//	func (d *DatabaseService) Start(ctx context.Context) error { return nil }
//	func (d *DatabaseService) Stop(ctx context.Context) error  { return nil }
//
//	type APIService struct{}
//	func (a *APIService) Name() string                      { return "api" }
//	func (a *APIService) Init(ctx context.Context) error    { return nil }
//	func (a *APIService) Start(ctx context.Context) error   { return nil }
//	func (a *APIService) Stop(ctx context.Context) error    { return nil }
//	func (a *APIService) Dependencies() []string            { return []string{"db"} }
//
//	func main() {
//	    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	    defer cancel()
//
//	    orch := lifecycle.NewOrchestrator()
//	    orch.Register(&DatabaseService{})
//	    orch.Register(&APIService{})
//
//	    if err := orch.InitAll(ctx); err != nil {
//	        panic(err)
//	    }
//	    if err := orch.StartAll(ctx); err != nil {
//	        panic(err)
//	    }
//	    defer orch.StopAll(ctx)
//	}
package lifecycle
