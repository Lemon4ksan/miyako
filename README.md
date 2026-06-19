<div align="center">

# 🌸 miyako

### The Metropolitan Concurrency & Tactical Response Engine for Go

[![Go Reference](https://img.shields.io/badge/go-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/lemon4ksan/miyako)
[![Go Report Card](https://goreportcard.com/badge/github.com/lemon4ksan/miyako?style=flat-square)](https://goreportcard.com/report/github.com/lemon4ksan/miyako)
[![License](https://img.shields.io/github/license/lemon4ksan/miyako?style=flat-square)](LICENSE)

> _"Every empire needs a capital. Where raw goroutines run like chaotic crowds, Miyako provides the absolute architecture, administrative order, and high-speed transit of a pristine metropolis."_

#### 🇺🇸 [English](README.md) • 🇷🇺 [Русский](README_RU.md)

</div>

### Why Miyako?

When building microservices, real-time event loops, or distributed worker pools in Go, managing concurrency can feel like navigating an unpredictable, sprawling wilderness. Without strict coordination, goroutine leaks, database race conditions, and resource deadlocks threaten to destabilize your entire system.

Named after the historic capital (**miyako** / 都) and built on the principles of metropolitan order and unwavering discipline, `miyako` is a high-performance Go toolkit. It acts as the **administrative backbone of your application** — providing structured service orchestrators (`lifecycle`), high-speed concurrent pipelines (`yumi`), and secure state guards (`keylock`) to govern millions of concurrent operations with absolute urban-grade precision.


```shell
go get github.com/lemon4ksan/miyako
```

## 🎯 When to Use Miyako vs. Standard Primitives

`miyako` is engineered for complex, high-risk coordination scenarios where state corruption or runtime deadlocks could cause system-wide crashes.

* **Choose standard `sync` / `channels`** for: Simple pipelines, basic worker pools, local mutex blocks, and standard short-lived async operations.
* **Choose `miyako`** for: Topologically sorted service boot sequences, correlation-ID job tracking, non-blocking type-safe event backbones, striped key-based locking with automatic cleanup, and quick-draw request deduplication. It is your **combat gear** for hostile concurrent environments.

## ⚡ The Contrast: Raw Go vs. `miyako`

Every `miyako` package replaces a pile of boilerplate, manual locking, and silent runtime failures with a concise, generics-first, thread-safe API. Here is what you are writing today versus what you could be writing.

### Job Tracking

<table width="100%">
<tr>
<th width="50%">Raw Go (Manual State Tracking)</th>
<th width="50%">Using <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
type Job struct {
    Done chan struct{}
    Err  error
}
mu.Lock()
jobs[id] = job
mu.Unlock()

go func() {
    select {
    case <-ctx.Done():
    case <-time.After(timeout):
    }
}()

// manual error propagation & wait loop
```

</td>
<td valign="top">

```go
mgr := jobs.NewManager[string, Result](capacity)

err := mgr.Add(jobID, callback,
    jobs.WithTimeout[Result](30*time.Second),
    jobs.WithContext[Result](ctx),
)
res, err := mgr.WaitFor(ctx, jobID)
```

</td>
</tr>
</table>

### Request Deduplication

<table width="100%">
<tr>
<th width="50%">Raw Go (Manual Dedup)</th>
<th width="50%">Using <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Panic in worker kills ALL waiters
// 🔴 No context cancellation
// 🔴 Manual map + mutex + channel wiring

var (
    mu   sync.Mutex
    inflight = map[string]*call{}
)

type call struct {
    wg  sync.WaitGroup
    val *User
    err error
}

func fetchUser(key string) (*User, error) {
    mu.Lock()
    if c, ok := inflight[key]; ok {
        mu.Unlock()
        c.wg.Wait()
        return c.val, c.err
    }
    c := &call{wg: sync.WaitGroup{1}}
    inflight[key] = c
    mu.Unlock()

    c.wg.Add(1)
    c.val, c.err = db.Fetch(key) // panic = all waiters die
    c.wg.Done()

    mu.Lock()
    delete(inflight, key)
    mu.Unlock()
    return c.val, c.err
}
```

</td>
<td valign="top">

```go
// ✅ Panic isolated to initiator only
// ✅ Context cancellation on all waiters
// ✅ Zero-value ready, no setup

group := &batto.Group[string, *User]{}

user, err := group.Do(ctx, "user-123",
    func(ctx context.Context) (*User, error) {
        return db.FetchUser(ctx, 123)
    },
)
```

</td>
</tr>
</table>

### Per-Key Locking

<table width="100%">
<tr>
<th width="50%">Raw Go (Memory-Leaky Lock Map)</th>
<th width="50%">Using <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Mutexes never cleaned up → memory leak
// 🔴 No TryLock, no ForceUnlock
// 🔴 Manual bookkeeping for every key

var (
    mu    sync.Mutex
    locks = map[string]*sync.Mutex{}
)

func getLock(key string) *sync.Mutex {
    mu.Lock()
    defer mu.Unlock()
    if locks[key] == nil {
        locks[key] = &sync.Mutex{}
    }
    return locks[key]
}

func processOrder(orderID string) {
    getLock(orderID).Lock()
    defer getLock(orderID).Unlock()
    // ... work ...
    // 🔑 lock entry stays in map forever
}
```

</td>
<td valign="top">

```go
// ✅ Auto-cleanup via refcount when key is released
// ✅ TryLock, ForceUnlock, Keys() built-in
// ✅ Generic key type: string, int, UUID, etc.

lock := keylock.New[string]()

func processOrder(orderID string) {
    lock.Lock(orderID)
    defer lock.Unlock(orderID)
    // ... work ...
    // 🔑 entry auto-deleted when refcount hits 0
}
```

</td>
</tr>
</table>

### Lazy Initialization

<table width="100%">
<tr>
<th width="50%">Raw Go (<code>sync.Once</code>)</th>
<th width="50%">Using <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 No reset - once broken, broken forever
// 🔴 Must store value + once separately

var (
    dbOnce sync.Once
    db     *sql.DB
)

func getDB() *sql.DB {
    dbOnce.Do(func() {
        var err error
        db, err = sql.Open("pg", dsn)
        if err != nil {
            // 🔴 dbOnce already marked done
            // 🔴 subsequent calls return nil, nil
        }
    })
    return db
}
// getDB() after error = nil forever
```

</td>
<td valign="top">

```go
// ✅ Reset re-runs initialization on next Get()
// ✅ Thread-safe, generic, zero-value ready

db := lazy.New(func() *sql.DB {
    conn, _ := sql.Open("pg", dsn)
    return conn
})

func getDB() *sql.DB { return db.Get() }

// After error recovery:
db.Reset() // next Get() retries initialization
```

</td>
</tr>
</table>

### Bulk Parallel Processing

<table width="100%">
<tr>
<th width="50%">Raw Go (Manual Fan-Out)</th>
<th width="50%">Using <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Order not preserved
// 🔴 No rate limiting
// 🔴 Manual WaitGroup + error collection

results := make([]Result, len(items))
var wg sync.WaitGroup
sem := make(chan struct{}, 10)

for _, item := range items {
    wg.Add(1)
    sem <- struct{}{}
    go func(it Item) {
        defer wg.Done()
        defer func() { <-sem }()
        r, err := process(ctx, it)
        // 🔴 race on results slice if no mutex
        results[idx] = r
    }(item)
}
wg.Wait()
// 🔴 results order != items order
```

</td>
<td valign="top">

```go
// ✅ Order preserved, rate-limited, fail-fast option
// ✅ One-liner for slice processing

results, err := yumi.Map(ctx, yumi.PipelineConfig{
    Workers: 10,
    RPS:     100,
    FailFast: true,
}, items, func(ctx context.Context, it Item) (Result, error) {
    return process(ctx, it)
})
// results order == items order
```

</td>
</tr>
</table>

### Concurrency Limiting

<table width="100%">
<tr>
<th width="50%">Raw Go (Static Semaphore)</th>
<th width="50%">Using <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Limit is fixed at creation
// 🔴 No dynamic resize
// 🔴 Zombie goroutines on ctx cancel

sem := make(chan struct{}, 10)

// Acquire
sem <- struct{}{}

// Release
<-sem

// 🔴 Changing limit requires recreating channel
// 🔴 ctx cancel doesn't unblock waiting goroutines
```

</td>
<td valign="top">

```go
// ✅ Dynamic resize without restart
// ✅ ctx cancellation unblocks waiters instantly
// ✅ Clean API

sem := semaphore.New(10)

if err := sem.Acquire(ctx); err != nil {
    return err // ctx cancelled → clean exit
}
defer sem.Release()

// Later: scale up/down at runtime
sem.Resize(20)
```

</td>
</tr>
</table>

### Behavior Orchestration

<table width="100%">
<tr>
<th width="50%">Raw Go (Manual Goroutine Management)</th>
<th width="50%">Using <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Manual goroutine tracking
// 🔴 No fail-fast, no graceful shutdown
// 🔴 Error in one goroutine = silent leak

var wg sync.WaitGroup
ctx, cancel := context.WithCancel(ctx)

wg.Add(2)

go func() {
    defer wg.Done()
    for {
        select {
        case <-ctx.Done():
            return
        default:
            if err := ticker(); err != nil {
                log.Println(err)
                // 🔴 other goroutines keep running
            }
        }
    }
}()

go func() {
    defer wg.Done()
    for {
        select {
        case <-ctx.Done():
            return
        default:
            if err := watcher(); err != nil {
                log.Println(err)
            }
        }
    }
}()

cancel()
wg.Wait()
```

</td>
<td valign="top">

```go
// ✅ Managed lifecycle with fail-fast
// ✅ Graceful shutdown, all goroutines tracked
// ✅ Logger integration, duplicate detection

orch := behavior.NewOrchestrator(
    behavior.WithLogger(myLogger),
    behavior.WithFailFast(),
)

orch.Register(&tickerBehavior{})
orch.Register(&watcherBehavior{})

ctx, cancel := context.WithCancel(ctx)
defer cancel()

orch.Start(ctx)
// ... later
orch.Stop() // all behaviors stopped cleanly
```

</td>
</tr>
</table>

### What Every Package Replaces

| Package | You write today (Raw Go) | `miyako` equivalent |
| :--- | :--- | :--- |
| `batto` | `sync.Mutex` + `map[string]*call` + `sync.WaitGroup` + panic recovery | `batto.Group[K, V]{}.Do(ctx, key, fn)` |
| `bus` | Channels per type + `reflect` switch + manual fan-out | `bus.New()` → `Subscribe` / `Publish` |
| `jobs` | Goroutine + `chan` + `sync.Mutex` + `time.After` + manual cleanup | `jobs.NewManager[K, T](n)` → `Add` / `WaitFor` |
| `lifecycle` | Hardcoded init order + manual rollback on failure | `lifecycle.NewOrchestrator()` → `Register` / `StartAll` |
| `scheduler` | `time.Ticker` + `sort.Slice` + manual wake-up loop | `scheduler.New()` → `Schedule` / `Start` |
| `yumi` | `sync.WaitGroup` + buffered channel + `sync.Mutex` for results | `yumi.Map(ctx, cfg, items, fn)` |
| `semaphore` | `make(chan struct{}, N)` - static, no cancel | `semaphore.New(n)` → `Acquire(ctx)` / `Resize` |
| `keylock` | `map[string]*sync.Mutex` - no cleanup, no TryLock | `keylock.New[K]()` → `Lock(key)` |
| `lazy` | `sync.Once` + separate `var` - no reset | `lazy.New(fn)` → `Get()` / `Reset()` |
| `spinlock` | `sync.Mutex` - heavier for short critical sections | `spinlock.SpinLock{}` → `Lock()` / `Unlock()` |
| `generic` | Duplicated `map`/`filter`/`retry` per package | `generic.Map`, `generic.Retry`, `generic.Future` |
| `behavior` | Goroutine + `sync.WaitGroup` + manual error propagation + shutdown coordination | `behavior.NewOrchestrator()` → `Register` / `Start` / `Stop` |

## 📊 Feature Matrix

This matrix shows where `miyako` focuses its design compared to Go's default primitives and generic wrappers:

| Feature / Capability | Go `sync` (StdLib) | Go `x/sync` (Experimental) | `miyako` |
| :--- | :---: | :---: | :---: |
| **Generics-first Design** | ✗ (Manual) | ✗ (Interface-based) | **✓ (Type-safe `[T]`)** |
| **Topological Startup & Shutdown** | ✗ | ✗ | **✓ (`lifecycle.Orchestrator`)** |
| **Correlation-ID Job Tracking** | ✗ | ✗ | **✓ (`jobs.Manager`)** |
| **Cancellable Resizable Semaphore** | ✗ | ⚠️ (Static only) | **✓ (`sync/semaphore.Semaphore`)** |
| **Request Deduplication** | ✗ | ⚠️ (basic SingleFlight) | **✓ (`batto.Group` / Quick-Draw)** |
| **Key-Based Striped Mutex** | ✗ | ✗ | **✓ (`sync/keylock.KeyMutex`)** |
| **Non-Blocking Type-Based Event Bus** | ✗ | ✗ | **✓ (`bus.Bus` / Type-Safe)** |
| **Reset-Aware Lazy Initializer** | ⚠️ (`sync.Once`) | ✗ | **✓ (`sync/lazy.Lazy`)** |
| **Ultra-Fast Spinlock Waiting** | ✗ | ✗ | **✓ (`sync/spinlock.SpinLock`)** |
| **Batching Request DataLoader** | ✗ | ✗ | **✓ (`generic.DataLoader`)** |
| **Strict Generic State Machine** | ✗ | ✗ | **✓ (`kata.FSM`)** |
| **Concurrent Behavior Orchestrator** | ✗ | ✗ | **✓ (`behavior.Orchestrator`)** |

## 🍳 The Concurrency Kata: Tactical Recipes

Here is how you solve common, frustrating concurrency and orchestration challenges using `miyako`.

### 1. Battojutsu Request Deduplication (`batto`)
* **The Problem:** Multiple incoming API requests query the same database entry concurrently, spawning expensive SQL calls. If the SQL query panics, standard singleflight will panic all waiting threads, or leak them.
* **The Solution:** Named after **Battojutsu** (拔刀术 / the art of quick-drawing a katana), the `batto` package cuts off duplicate concurrent calls. If the worker panics, the panic is safely isolated and propagated *only* to the initiating thread, while secondary waiters receive a clean `ErrWorkerPanicked`.

```go
group := &batto.Group[string, *User]{}

user, err := group.Do(ctx, "user-123", func(workerCtx context.Context) (*User, error) {
    // Spawns exactly once for concurrent requests to "user-123"
    return db.FetchUser(workerCtx, 123)
})
```

### 2. Topologically Sorted Service Bootstrapping (`lifecycle`)
* **The Problem:** Your microservice needs `Database` started first, then `RedisCache` (which depends on database), and finally the `WebServer` (which depends on both). During shutdown, they must stop in the exact reverse order.
* **The Solution:** `lifecycle` uses a Depth-First Search (DFS) algorithm to sort and initialize your services, resolving dependencies and rolling back automatically on failure.

```go
orchestrator := lifecycle.NewOrchestrator()

// Register services. Dependents declare their dependencies.
orchestrator.Register(NewDatabaseService())
orchestrator.Register(NewRedisCacheService()) // Dependencies() -> []string{"db"}
orchestrator.Register(NewWebServerService())  // Dependencies() -> []string{"db", "redis"}

// Sorts topologically and initializes all services
if err := orchestrator.InitAll(ctx); err != nil {
    log.Fatalf("Init failed: %v", err)
}

// Starts all services. On any failure, successfully started services rollback in reverse.
if err := orchestrator.StartAll(ctx); err != nil {
    log.Fatalf("Start failed: %v", err)
}

// Gracefully shuts down in reverse topological order: WebServer -> RedisCache -> Database
defer orchestrator.StopAll(context.Background())
```

### 3. Dynamic Concurrency Gating (Cancellable Resizable Semaphore)
* **The Problem:** Your worker pool needs to limit concurrent calls to an upstream API. The API capacity changes dynamically during runtime, and waiting workers must unblock immediately if their context is canceled.
* **The Solution:** `sync/semaphore` manages dynamic limits and includes bulletproof context cancellation, preventing zombie channels from leaking memory.

```go
// Create a semaphore with an initial limit of 10 concurrent requests
sem := semaphore.New(10)

go func() {
    // Adapt limit dynamically based on upstream API health signals
    time.Sleep(1 * time.Minute)
    sem.Resize(5) // Scale down capacity to 5 slots
}()

// Acquire slot respects context cancellation without leaving zombie channels
if err := sem.Acquire(ctx); err != nil {
    return err // Context cancelled, worker exits cleanly
}
defer sem.Release()

api.Call()
```

### 4. Striped Locking with Auto-Teardown (`sync/keylock`)
* **The Problem:** You want to serialize operations per user ID. Storing a standard `sync.Mutex` per user in a global map will cause a memory leak as users connect and disconnect.
* **The Solution:** `keylock` manages dynamic mutexes and automatically cleans them up from memory once the reference count of waiting goroutines drops to zero.

```go
lock := keylock.New[string]()

func ProcessUserRecord(userID string) {
    lock.Lock(userID)
    defer lock.Unlock(userID) // KeyMutex automatically deletes key from map when count == 0
    
    // Serialized user record modifications
}
```

### 5. Automated Rate-Limited Batching (`generic.DataLoader`)
* **The Problem:** Multiple goroutines query individual item metrics concurrently, causing your API client to get rate-limited.
* **The Solution:** Named after **Yumi** (弓 / the Japanese bow), `generic.DataLoader` collects individual queries over a 5ms window, batches them into a single bulk query, and distributes results back to each goroutine.

```go
loader := yumi.NewDataLoader[string, *Price](5*time.Millisecond, func(ctx context.Context, keys []string) (map[string]*Price, error) {
    return pricedbClient.GetItemsBulk(ctx, keys) // Executed exactly once!
})

// Spawning this concurrently in 10 goroutines will trigger only ONE API call!
price, err := loader.Load(ctx, "item_sku")
```

### 6. Strict Generic State Machine (`kata`)
* **The Problem:** You need to model a lifecycle (e.g. order processing, connection states, bot flow) where invalid transitions must be caught at compile time, and all state changes must be thread-safe with transactional rollback support.
* **The Solution:** `kata` provides a strictly typed FSM parameterized over comparable `State` and `Event` generics. It supports before/after hooks with rollback, concurrent-safe transitions, and automatic Graphviz DOT export.

```go
type State int
const (
    Idle State = iota
    Running
    Stopped
)

type Event int
const (
    Start Event = iota
    Stop
)

fsm := kata.NewFSM[State, Event](Idle)

fsm.AddRules(
    kata.TransitionRule[State, Event]{From: Idle, Event: Start, To: Running},
    kata.TransitionRule[State, Event]{From: Running, Event: Stop, To: Stopped},
)

// Before-hook: abort transition if preconditions fail
fsm.OnBefore(Start, func(ctx context.Context, from State, event Event, to State) error {
    if !healthCheckOK(ctx) {
        return errors.New("upstream unhealthy, blocking start")
    }
    return nil
})

// Thread-safe from any goroutine
err := fsm.Transition(context.Background(), Start)

// Export visual diagram
fmt.Println(fsm.ToDOT())
```

### 7. Concurrent Behavior Orchestration (`behavior`)
* **The Problem:** You need to run multiple independent background tasks (ticker, watcher, health-check) in parallel, with coordinated shutdown and optional fail-fast - but manual `sync.WaitGroup` + `context.WithCancel` wiring is error-prone and untestable.
* **The Solution:** `behavior` provides an `Orchestrator` that manages the lifecycle of registered `Behavior` instances. Each runs in its own goroutine with automatic tracking, graceful shutdown, and optional fail-fast mode.

```go
type tickerBehavior struct {
    name     string
    interval time.Duration
}

func (t *tickerBehavior) Name() string { return t.name }

func (t *tickerBehavior) Run(ctx context.Context) error {
    ticker := time.NewTicker(t.interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return nil
        case <-ticker.C:
            fmt.Printf("Tick from %s\n", t.name)
        }
    }
}

orch := behavior.NewOrchestrator(
    behavior.WithFailFast(),
)

orch.Register(&tickerBehavior{name: "fast", interval: time.Second})
orch.Register(&tickerBehavior{name: "slow", interval: 5 * time.Second})

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

orch.Start(ctx)
// ... later
orch.Stop() // all behaviors stopped cleanly
```

## 🔬 The Contrast: Raw FSM vs. `kata`

A finite state machine without generics forces you into `interface{}` or `string` states, manual locking, and scattered transition logic. Here is what that looks like:

<table width="100%">
<tr>
<th width="50%">Raw Go FSM (Boilerplate & Unsafe)</th>
<th width="50%">Using <code>kata</code> (Generics & Thread-Safe)</th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 No compile-time safety: states are strings
// 🔴 Manual mutex on every access
// 🔴 No hooks, no rollback, no visualization

type RawFSM struct {
    mu      sync.RWMutex
    current string
    rules   map[string]map[string]string
}

func (f *RawFSM) Transition(event string) error {
    f.mu.Lock()
    defer f.mu.Unlock()

    events, ok := f.rules[f.current]
    if !ok {
        return fmt.Errorf("no rules for %s", f.current)
    }
    to, ok := events[event]
    if !ok {
        return fmt.Errorf("invalid: %s + %s", f.current, event)
    }
    f.current = to
    return nil
}

// Usage: easy to typo, no compiler help
fsm := &RawFSM{
    current: "idle",
    rules: map[string]map[string]string{
        "idle": {"start": "running"},
    },
}
fsm.Transition("statr") // typo - compiles fine, runtime error
```

</td>
<td valign="top">

```go
// ✅ Compile-time safe: wrong state = build error
// ✅ Thread-safe by design, no manual locks
// ✅ Before/after hooks, rollback, DOT export

fsm := kata.NewFSM[State, Event](Idle)

fsm.AddRules(
    kata.TransitionRule[State, Event]{
        From: Idle, Event: Start, To: Running,
    },
)

fsm.OnBefore(Start, func(ctx context.Context,
    from State, event Event, to State) error {
    return db.BeginTx(ctx) // rollback on error
})

// Usage: typos caught at compile time
fsm.Transition(Start) // OK
fsm.Transition(Statr) // COMPILE ERROR
```

</td>
</tr>
</table>

**What you get with `kata` that raw implementations lack:**

| Concern | Raw Go FSM | `kata` FSM |
| :--- | :--- | :--- |
| **Type safety** | `string` / `interface{}` - typos are silent | Generic `[State, Event]` - wrong types won't compile |
| **Thread safety** | Manual `sync.Mutex` on every method | Built-in `sync.RWMutex`, lock-free reads |
| **Transition hooks** | Scattered `if` checks before/after | `OnBefore` / `OnAfter` with rollback support |
| **Transactional rollback** | Manual flag + restore on error | Before-hook error aborts atomically |
| **Validation** | Runtime panic or silent miss | `Validate()` + compile-time guarantees |
| **Visualization** | Draw diagrams by hand | `ToDOT()` - one line, render with Graphviz |
| **Test setup** | Rewrite transition logic in test helpers | `ForceSet()` - direct state injection |

## ⚖️ Legal & License

This project is licensed under the **BSD 3-Clause License**. See [LICENSE](LICENSE) for full details.

<div align="center">
  <sub>Keep a cold head, protect the capital. Discipline of Section 6.</sub>
</div>
