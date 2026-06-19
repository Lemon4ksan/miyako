<div align="center">

# 🌸 miyako

### Столичный движок конкурентности и тактического реагирования для Go

[![Go Reference](https://img.shields.io/badge/go-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/lemon4ksan/miyako)
[![Go Report Card](https://goreportcard.com/badge/github.com/lemon4ksan/miyako?style=flat-square)](https://goreportcard.com/report/github.com/lemon4ksan/miyako)
[![License](https://img.shields.io/github/license/lemon4ksan/miyako?style=flat-square)](LICENSE)

> _«Каждой империи нужна столица. Там, где обычные горутины бегут хаотичной толпой, Miyako развертывает безупречную архитектуру, порядок и скоростную инфраструктуру мегаполиса»._

#### 🇺🇸 [English](README.md) • 🇷🇺 [Русский](README_RU.md)

</div>

### Почему Miyako?

При проектировании микросервисов, циклов обработки событий реального времени или распределенных пулов воркеров в Go управление конкурентностью может напоминать блуждание по диким, нецивилизованным землям. Без жесткой координирующей силы утечки горутин, состояния гонки в базах данных и взаимные блокировки потоков постоянно угрожают стабильности вашей системы.

Названный в честь столицы (**miyako** / 都) и построенный на принципах жесткого централизованного планирования и дисциплины, `miyako` — это высокопроизводительный инструментарий для Go. Он выступает в роли **административного сердца вашего приложения** — предоставляя структурированные оркестраторы сервисов (`lifecycle`), скоростные многопоточные конвейеры данных (`yumi`) и безопасные блокировки состояния (`keylock`), позволяющие управлять миллионами параллельных операций с абсолютной точностью современного мегаполиса.

```shell
go get github.com/lemon4ksan/miyako
```

## 🎯 Когда использовать Miyako вместо стандартных примитивов

`miyako` спроектирован для сложных сценариев координации с высокими рисками, где повреждение состояния или системные взаимоблокировки могут привести к каскадному падению сервисов.

* **Выбирайте стандартные `sync` / `channels`** для: простых конвейеров, базовых пулов воркеров, локальных мьютексов и стандартных короткоживущих асинхронных операций.
* **Выбирайте `miyako`** для: топологически отсортированных последовательностей запуска сервисов, отслеживания задач по Correlation ID, неблокирующих потокобезопасных шин событий, секционированных блокировок по ключу с автоматической очисткой и быстрого подавления дублирующихся запросов. Это ваше **боевое снаряжение** для работы во враждебных конкурентных средах.

## ⚡ Контраст: Сырой Go vs. `miyako`

Каждый пакет `miyako` заменяет кучу шаблонного кода, ручных блокировок и молчаливых ошибок runtime на лаконичный, типобезопасный API. Вот что вы пишете сегодня versus что могли бы писать.

### Отслеживание задач

<table width="100%">
<tr>
<th width="50%">Сырой Go (Ручное отслеживание состояния)</th>
<th width="50%">Использование <code>miyako</code></th>
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

// ручное распространение ошибок и цикл ожидания
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

### Дедупликация запросов

<table width="100%">
<tr>
<th width="50%">Сырой Go (Ручная дедупликация)</th>
<th width="50%">Использование <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Паника в воркере убивает ВСЕХ ожидающих
// 🔴 Нет отмены через контекст
// 🔴 Ручная намотка: map + mutex + channel

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
    c := &call{}
    inflight[key] = c
    mu.Unlock()

    c.wg.Add(1)
    c.val, c.err = db.Fetch(key)
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
// ✅ Паника изолирована только для инициатора
// ✅ Отмена контекста для всех ожидающих
// ✅ Готов к использованию с нулевого значения

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

### Блокировка по ключу

<table width="100%">
<tr>
<th width="50%">Сырой Go (Утечка памяти в map мьютексов)</th>
<th width="50%">Использование <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Мьютексы никогда не удаляются → утечка памяти
// 🔴 Нет TryLock, нет ForceUnlock
// 🔴 Ручной учёт для каждого ключа

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
    // ... работа ...
    // 🔑 запись навсегда остаётся в map
}
```

</td>
<td valign="top">

```go
// ✅ Автоочистка через счётчик ссылок
// ✅ TryLock, ForceUnlock, Keys() встроены
// ✅ Дженерик-тип ключа: string, int, UUID и т.д.

lock := keylock.New[string]()

func processOrder(orderID string) {
    lock.Lock(orderID)
    defer lock.Unlock(orderID)
    // ... работа ...
    // 🔑 запись удаляется при refcount == 0
}
```

</td>
</tr>
</table>

### Ленивая инициализация

<table width="100%">
<tr>
<th width="50%">Сырой Go (<code>sync.Once</code>)</th>
<th width="50%">Использование <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Нет сброса - сломалось один раз, сломано навсегда
// 🔴 Значение и Once хранятся отдельно

var (
    dbOnce sync.Once
    db     *sql.DB
)

func getDB() *sql.DB {
    dbOnce.Do(func() {
        var err error
        db, err = sql.Open("pg", dsn)
        if err != nil {
            // 🔴 Once уже помечен как выполненный
            // 🔴 последующие вызовы вернут nil, nil
        }
    })
    return db
}
// getDB() после ошибки = nil навсегда
```

</td>
<td valign="top">

```go
// ✅ Reset перезапускает инициализацию при следующем Get()
// ✅ Потокобезопасный, дженерик, нулевое значение готово

db := lazy.New(func() *sql.DB {
    conn, _ := sql.Open("pg", dsn)
    return conn
})

func getDB() *sql.DB { return db.Get() }

// После восстановления из ошибки:
db.Reset() // следующий Get() повторит инициализацию
```

</td>
</tr>
</table>

### Пакетная параллельная обработка

<table width="100%">
<tr>
<th width="50%">Сырой Go (Ручной Fan-Out)</th>
<th width="50%">Использование <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Порядок не сохраняется
// 🔴 Нет ограничения частоты
// 🔴 Ручной WaitGroup + сбор ошибок

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
        // 🔴 гонка на results без мьютекса
        results[idx] = r
    }(item)
}
wg.Wait()
// 🔴 порядок results != порядок items
```

</td>
<td valign="top">

```go
// ✅ Порядок сохраняется, ограничение частоты, FailFast
// ✅ Одна строка для обработки среза

results, err := yumi.Map(ctx, yumi.PipelineConfig{
    Workers: 10,
    RPS:     100,
    FailFast: true,
}, items, func(ctx context.Context, it Item) (Result, error) {
    return process(ctx, it)
})
// порядок results == порядок items
```

</td>
</tr>
</table>

### Ограничение конкурентности

<table width="100%">
<tr>
<th width="50%">Сырой Go (Статический семафор)</th>
<th width="50%">Использование <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Лимит фиксирован при создании
// 🔴 Нет динамического изменения
// 🔴 Зомби-горутины при отмене ctx

sem := make(chan struct{}, 10)

// Захват
sem <- struct{}{}

// Освобождение
<-sem

// 🔴 Изменение лимита = пересоздание канала
// 🔴 Отмена ctx не разблокирует ожидающих
```

</td>
<td valign="top">

```go
// ✅ Динамическое изменение без перезапуска
// ✅ Отмена ctx мгновенно разблокирует ожидающих
// ✅ Чистый API

sem := semaphore.New(10)

if err := sem.Acquire(ctx); err != nil {
    return err // ctx отменён → чистый выход
}
defer sem.Release()

// Позже: масштабирование в runtime
sem.Resize(20)
```

</td>
</tr>
</table>

### Оркестрация конкурентных поведений

<table width="100%">
<tr>
<th width="50%">Сырой Go (Ручное управление горутинами)</th>
<th width="50%">Использование <code>miyako</code></th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Ручное отслеживание горутин
// 🔴 Нет fail-fast, нет корректной остановки
// 🔴 Ошибка в одной горутине = молчаливая утечка

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
                // 🔴 другие горутины продолжают работу
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
// ✅ Управляемый жизненный цикл с fail-fast
// ✅ Корректная остановка, все горутины отслеживаются
// ✅ Интеграция с логгером, детекция дубликатов

orch := behavior.NewOrchestrator(
    behavior.WithLogger(myLogger),
    behavior.WithFailFast(),
)

orch.Register(&tickerBehavior{})
orch.Register(&watcherBehavior{})

ctx, cancel := context.WithCancel(ctx)
defer cancel()

orch.Start(ctx)
// ... позже
orch.Stop() // все поведения корректно остановлены
```

</td>
</tr>
</table>

### Что заменяет каждый пакет

| Пакет | Вы пишете сегодня (Сырой Go) | Эквивалент в `miyako` |
| :--- | :--- | :--- |
| `batto` | `sync.Mutex` + `map[string]*call` + `sync.WaitGroup` + восстановление после паники | `batto.Group[K, V]{}.Do(ctx, key, fn)` |
| `bus` | Каналы на тип + `reflect`-switch + ручной fan-out | `bus.New()` → `Subscribe` / `Publish` |
| `jobs` | Горутина + `chan` + `sync.Mutex` + `time.After` + ручная очистка | `jobs.NewManager[K, T](n)` → `Add` / `WaitFor` |
| `lifecycle` | Захардкоженный порядок инициализации + ручной rollback | `lifecycle.NewOrchestrator()` → `Register` / `StartAll` |
| `scheduler` | `time.Ticker` + `sort.Slice` + ручной цикл пробуждения | `scheduler.New()` → `Schedule` / `Start` |
| `yumi` | `sync.WaitGroup` + буферизированный канал + `sync.Mutex` для результатов | `yumi.Map(ctx, cfg, items, fn)` |
| `semaphore` | `make(chan struct{}, N)` - статический, без отмены | `semaphore.New(n)` → `Acquire(ctx)` / `Resize` |
| `keylock` | `map[string]*sync.Mutex` - без очистки, без TryLock | `keylock.New[K]()` → `Lock(key)` |
| `lazy` | `sync.Once` + отдельная `var` - без сброса | `lazy.New(fn)` → `Get()` / `Reset()` |
| `spinlock` | `sync.Mutex` - тяжелее для коротких секций | `spinlock.SpinLock{}` → `Lock()` / `Unlock()` |
| `generic` | Дублированные `map`/`filter`/`retry` в каждом пакете | `generic.Map`, `generic.Retry`, `generic.Future` |
| `behavior` | Горутина + `sync.WaitGroup` + ручное распространение ошибок + координация остановки | `behavior.NewOrchestrator()` → `Register` / `Start` / `Stop` |

## 📊 Матрица возможностей

Эта таблица показывает фокус проектирования `miyako` в сравнении с дефолтными примитивами Go и базовыми обертками:

| Возможность / Функция | Go `sync` (StdLib) | Go `x/sync` (Экспериментальная) | `miyako` |
| :--- | :---: | :---: | :---: |
| **Проектирование на основе Generics** | ✗ (Вручную) | ✗ (На базе интерфейсов) | **✓ (Типобезопасный `[T]`)** |
| **Топологический запуск и остановка** | ✗ | ✗ | **✓ (`lifecycle.Orchestrator`)** |
| **Отслеживание задач по Correlation ID** | ✗ | ✗ | **✓ (`jobs.Manager`)** |
| **Отменяемый семафор с динамическим размером** | ✗ | ⚠️ (Только статический) | **✓ (`sync/semaphore.Semaphore`)** |
| **Дедупликация запросов** | ✗ | ⚠️ (Базовый SingleFlight) | **✓ (`batto.Group` / Quick-Draw)** |
| **Секционированный мьютекс по ключу** | ✗ | ✗ | **✓ (`sync/keylock.KeyMutex`)** |
| **Неблокирующая шина событий на основе типов** | ✗ | ✗ | **✓ (`bus.Bus` / Типобезопасный)** |
| **Ленивый инициализатор с поддержкой сброса** | ⚠️ (`sync.Once`) | ✗ | **✓ (`sync/lazy.Lazy`)** |
| **Ультрабыстрое ожидание через Spinlock** | ✗ | ✗ | **✓ (`sync/spinlock.SpinLock`)** |
| **Пакетная загрузка данных (DataLoader)** | ✗ | ✗ | **✓ (`generic.DataLoader`)** |
| **Строгий типизированный конечный автомат** | ✗ | ✗ | **✓ (`kata.FSM`)** |
| **Оркестратор конкурентных поведений** | ✗ | ✗ | **✓ (`behavior.Orchestrator`)** |

## 🍳 Ката конкурентности: Тактические рецепты

Вот как можно элегантно решать типичные и сложные проблемы конкурентности и оркестрации с помощью `miyako`.

### 1. Дедупликация запросов Battojutsu (`batto`)
* **Проблема:** Множество входящих API-запросов одновременно запрашивают одну и ту же запись из базы данных, порождая тяжелые SQL-вызовы. Если SQL-запрос паникует, стандартный `singleflight` вызовет панику во всех ожидающих потоках или приведет к их утечке.
* **Решение:** Названный в честь **Баттодзюцу** (拔刀术 / искусство мгновенного обнажения меча), пакет `batto` отсекает дублирующие конкурентные вызовы. Если воркер паникует, паника безопасно изолируется и передается *только* инициирующему потоку, в то время как остальные ожидающие получают чистую ошибку `ErrWorkerPanicked`.

```go
group := &batto.Group[string, *User]{}

user, err := group.Do(ctx, "user-123", func(workerCtx context.Context) (*User, error) {
    // Запускается ровно один раз для всех конкурентных запросов к «user-123»
    return db.FetchUser(workerCtx, 123)
})
```

### 2. Топологически отсортированный запуск сервисов (`lifecycle`)
* **Проблема:** Вашему микросервису требуется сначала запустить `Database`, затем `RedisCache` (который зависит от базы данных) и, наконец, `WebServer` (который зависит от обоих компонентов). При завершении работы они должны останавливаться в строго обратном порядке.
* **Решение:** `lifecycle` использует алгоритм поиска в глубину (DFS) для сортировки и инициализации ваших сервисов, автоматически разрешая зависимости и выполняя откат (rollback) в случае сбоя.

```go
orchestrator := lifecycle.NewOrchestrator()

// Регистрация сервисов. Зависимые сервисы объявляют свои зависимости.
orchestrator.Register(NewDatabaseService())
orchestrator.Register(NewRedisCacheService()) // Dependencies() -> []string{"db"}
orchestrator.Register(NewWebServerService())  // Dependencies() -> []string{"db", "redis"}

// Выполняет топологическую сортировку и инициализирует все сервисы
if err := orchestrator.InitAll(ctx); err != nil {
    log.Fatalf("Init failed: %v", err)
}

// Запускает все сервисы. При любом сбое уже запущенные сервисы откатываются в обратном порядке.
if err := orchestrator.StartAll(ctx); err != nil {
    log.Fatalf("Start failed: %v", err)
}

// Корректно останавливает сервисы в обратном топологическом порядке: WebServer -> RedisCache -> Database
defer orchestrator.StopAll(context.Background())
```

### 3. Динамическое управление конкурентностью (Cancellable Resizable Semaphore)
* **Проблема:** Вашему пулу воркеров необходимо ограничить количество одновременных вызовов внешнего API. Пропускная способность API динамически меняется во время работы приложения, а ожидающие воркеры должны мгновенно разблокироваться при отмене их контекста.
* **Решение:** `sync/semaphore` управляет динамическими лимитами и обеспечивает надежную отмену через контекст, предотвращая утечки памяти из-за зависших каналов.

```go
// Создание семафора с начальным лимитом в 10 одновременных запросов
sem := semaphore.New(10)

go func() {
    // Динамическое изменение лимита на основе сигналов о состоянии внешнего API
    time.Sleep(1 * time.Minute)
    sem.Resize(5) // Снижение пропускной способности до 5 слотов
}()

// Захват слота учитывает отмену контекста, не оставляя «зомби»-каналов
if err := sem.Acquire(ctx); err != nil {
    return err // Контекст отменен, воркер корректно завершает работу
}
defer sem.Release()

api.Call()
```

### 4. Секционированные блокировки по ключу с автоочисткой (`sync/keylock`)
* **Проблема:** Вам необходимо сериализовать операции по ID пользователя. Хранение стандартного `sync.Mutex` для каждого пользователя в глобальной карте (map) приведет к утечке памяти по мере подключения и отключения пользователей.
* **Решение:** `keylock` управляет динамическими мьютексами и автоматически удаляет их из памяти, как только счетчик ссылок ожидающих горутин падает до нуля.

```go
lock := keylock.New[string]()

func ProcessUserRecord(userID string) {
    lock.Lock(userID)
    defer lock.Unlock(userID) // KeyMutex автоматически удаляет ключ из карты, когда счетчик равен 0
    
    // Сериализованные изменения записи пользователя
}
```

### 5. Автоматическая пакетная загрузка с ограничением частоты (`generic.DataLoader`)
* **Проблема:** Множество горутин одновременно запрашивают метрики отдельных товаров, из-за чего ваш API-клиент быстро исчерпывает лимиты запросов (rate limit).
* **Решение:** Названный в честь **Юми** (弓 / японский лук), `generic.DataLoader` собирает индивидуальные запросы в течение скользящего окна (например, 5 мс), объединяет их в один пакетный запрос и распределяет результаты обратно по горутинам.

```go
loader := yumi.NewDataLoader[string, *Price](5*time.Millisecond, func(ctx context.Context, keys []string) (map[string]*Price, error) {
    return pricedbClient.GetItemsBulk(ctx, keys) // Выполняется ровно один раз!
})

// Запуск этого кода конкурентно в 10 горутинах вызовет только ОДИН API-запрос!
price, err := loader.Load(ctx, "item_sku")
```

### 6. Строгий типизированный конечный автомат (`kata`)
* **Проблема:** Вам необходимо смоделировать жизненный цикл (например, обработку заказов, состояния соединений, логику бота), где некорректные переходы должны выявляться еще на этапе компиляции, а все изменения состояния должны быть потокобезопасными и поддерживать транзакционный откат.
* **Решение:** `kata` предоставляет строго типизированный конечный автомат (FSM), параметризованный сравниваемыми (comparable) дженериками `State` (Состояние) и `Event` (Событие). Он поддерживает хуки «до» и «после» с возможностью отката, потокобезопасные переходы и автоматический экспорт диаграмм в формат Graphviz DOT.

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

// Хук «До»: прерывает переход, если предусловия не выполнены
fsm.OnBefore(Start, func(ctx context.Context, from State, event Event, to State) error {
    if !healthCheckOK(ctx) {
        return errors.New("upstream unhealthy, blocking start")
    }
    return nil
})

// Потокобезопасно при вызове из любой горутины
err := fsm.Transition(context.Background(), Start)

// Экспорт визуальной диаграммы
fmt.Println(fsm.ToDOT())
```

### 7. Оркестрация конкурентных поведений (`behavior`)
* **Проблема:** Вам необходимо запустить несколько независимых фоновых задач (тикер, вотчер, проверка здоровья) параллельно, с координированной остановкой и опциональным fail-fast - но ручная намотка `sync.WaitGroup` + `context.WithCancel` подвержена ошибкам и плохо тестируется.
* **Решение:** `behavior` предоставляет `Orchestrator`, управляет жизненным циклом зарегистрированных экземпляров `Behavior`. Каждый запускается в отдельной горутине с автоматическим отслеживанием, корректной остановкой и опциональным режимом fail-fast.

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
// ... позже
orch.Stop() // все поведения корректно остановлены
```

## 🔬 Контраст: Сырой FSM vs. `kata`

Конечный автомат без дженериков вынуждает использовать `interface{}` или `string`-состояния, ручные блокировку и рассеянную логику переходов. Вот как это выглядит:

<table width="100%">
<tr>
<th width="50%">Сырой FSM на Go (Шаблонный код & Небезопасно)</th>
<th width="50%">Использование <code>kata</code> (Дженерики & Потокобезопасно)</th>
</tr>
<tr>
<td valign="top">

```go
// 🔴 Нет безопасности на этапе компиляции: состояния - строки
// 🔴 Ручной мьютекс при каждом обращении
// 🔴 Нет хуков, нет отката, нет визуализации

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

// Использование: легко допустить опечатку, нет помощи компилятора
fsm := &RawFSM{
    current: "idle",
    rules: map[string]map[string]string{
        "idle": {"start": "running"},
    },
}
fsm.Transition("statr") // опечатка - компилируется, ошибка в runtime
```

</td>
<td valign="top">

```go
// ✅ Безопасность на этапе компиляции: неверное состояние = ошибка сборки
// ✅ Потокобезопасность «из коробки», без ручных блокировок
// ✅ Хуки «до/после», откат, экспорт в DOT

fsm := kata.NewFSM[State, Event](Idle)

fsm.AddRules(
    kata.TransitionRule[State, Event]{
        From: Idle, Event: Start, To: Running,
    },
)

fsm.OnBefore(Start, func(ctx context.Context,
    from State, event Event, to State) error {
    return db.BeginTx(ctx) // откат при ошибке
})

// Использование: опечатки ловятся на этапе компиляции
fsm.Transition(Start) // OK
fsm.Transition(Statr) // ОШИБКА КОМПИЛЯЦИИ
```

</td>
</tr>
</table>

**Что даёт `kata`, чего нет в сырой реализации:**

| Задача | Сырой FSM на Go | `kata` FSM |
| :--- | :--- | :--- |
| **Безопасность типов** | `string` / `interface{}` - опечатки молчаливы | Дженерики `[State, Event]` - неверные типы не компилируются |
| **Потокобезопасность** | Ручной `sync.Mutex` в каждом методе | Встроенный `sync.RWMutex`, блокировки только на запись |
| **Хуки переходов** | Рассеянные проверки `if` до/после | `OnBefore` / `OnAfter` с поддержкой отката |
| **Транзакционный откат** | Ручные флаги + восстановление при ошибке | Ошибка before-хука атомарно отменяет переход |
| **Валидация** | Runtime-паника или молчаливый пропуск | `Validate()` + гарантии на этапе компиляции |
| **Визуализация** | Рисовать диаграммы вручную | `ToDOT()` - одна строка, рендер через Graphviz |
| **Настройка тестов** | Переписывать логику переходов в хелперах | `ForceSet()` - прямая инъекция состояния |

## ⚖️ Лицензия

Этот проект распространяется под лицензией **BSD 3-Clause License**. Подробности см. в файле [LICENSE](LICENSE).

<div align="center">
  <sub>Сохраняйте хладнокровие, защищайте столицу. Дисциплина 6-го отдела.</sub>
</div>
