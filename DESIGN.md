# SEDA Bus — Design

A small, broker-less, **staged** message bus, implemented seven times over —
in Java, Rust, Python, TypeScript, C++, C#, and Go — from one shared design.
This document describes that shared design, then the ways each
implementation necessarily diverges from it.

- [`seda-bus-java`](../seda-bus-java/) — `1.3.1`; the original. Optional
  guaranteed-delivery persistence. Depends on `ra-common`.
- [`seda-bus-rust`](../seda-bus-rust/) — `0.3.0`; zero-dependency (only `log`), a
  real shared OS-thread pool. The one port not yet rewired onto `ra-common`'s
  `Envelope` — still its own standalone struct.
- [`seda-bus-python`](../seda-bus-python/) — `0.2.0`; depends on `ra-common`
  (carries `ra_common.Envelope` as of a `0.2.0` rewire); built to exercise
  free-threaded (PEP 703) CPython.
- [`seda-bus-ts`](../seda-bus-ts/) — `0.2.0`; depends on `@resolvingarchitecture/ra-common`
  (same `0.2.0` rewire); event-loop model with an optional `Worker`-thread
  transport for CPU-bound stages.
- [`seda-bus-cpp`](../seda-bus-cpp/) — `0.1.0`; header-only C++20, depends on
  `ra-common-cpp` (carries `ra::common::Envelope`). Follows `seda-bus-rust`'s
  concurrency model almost mechanically (real OS threads, hand-rolled pool,
  atomic-CAS permits) but Python/TS's envelope choice.
- [`seda-bus-cs`](../seda-bus-cs/) — `0.1.0`; C# / .NET 8, depends on
  `ra-common-cs` (carries `Ra.Common.Envelope`). Follows `seda-bus-java`'s
  concurrency model instead — the shared, built-in `ThreadPool` and a real
  `SemaphoreSlim`, not a hand-rolled pool — since .NET has both, unlike
  Rust/C++.
- [`seda-bus-go`](../seda-bus-go/) — `0.1.0`; depends on `ra-common-go` (carries
  `messaging.Envelope`, aliased as `sedabus.Envelope`). Needs no worker pool
  at all, hand-rolled or borrowed — goroutines are cheap enough that each
  scheduled drain is just `go bus.drain(ch)`, bounded by a buffered-channel
  semaphore instead of a sized pool.

Each subproject also carries its own `README.md`; the Java, TypeScript, C++,
C#, and Go subprojects carry a longer `DESIGN.md` of their own that this
document does not duplicate.

See [`CORRECTNESS_SUITE.md`](CORRECTNESS_SUITE.md) for the fixed list of
behavioral properties (backpressure, retry/dead-letter, consumer-failure
isolation, shutdown accounting, config validation, resource lifecycle,
concurrency correctness) every port's own test suite verifies, in its own
test framework — separate from `seda-bus-compare`'s throughput/latency
benchmark, which measures performance, not correctness.

Reference: Welsh, Culler, Brewer. *SEDA: An Architecture for Well-Conditioned,
Scalable Internet Services.* SOSP 2001.

---

## 1. The general design

### 1.1 What it is

Work is decomposed into **stages**. A stage is a named `Channel` (Java:
`MessageChannel`) holding a **bounded queue** and a list of **consumers**.
Producers publish envelopes; they do not call consumers. The queue between them
is the decoupling point and the place where load is conditioned.

One **shared worker pool** drains every stage. Each stage has its own
**concurrency limit** — the number of envelopes it may process at once — so no
single stage can monopolise the pool. This is the one structural departure from
the 2001 paper, which gave every stage its own dedicated pool; Welsh's own
retrospective concluded the per-stage-pool split was usually a mistake, and all
seven implementations avoid it — six via one shared worker pool, `seda-bus-go`
via goroutines bounded by a shared semaphore instead of a pool at all (see
§2.8; goroutines are cheap enough that there's nothing to pool).

Everything is in-process. There is no broker, no network hop, no persistence
broker. The only external moving part is the worker pool.

### 1.2 Components

```
Bus  (Java: SEDABus)
  channels        registry keyed by name
  callbacks       producer completion callbacks, keyed by envelope id
  dlq             source-channel -> dead-letter-channel name map
  pool            the one shared worker pool
  publish(env)    -> look up channel by env.to -> channel.offer(env) -> schedule(channel)

Channel  (one stage)
  capacity        admission control; a full queue triggers the back-pressure policy
  concurrency     how many drain tasks may run for this stage at once (a permit count)
  delivery        PointToPoint (round-robin over consumers) | PubSub (fan-out)
  backpressure    Block | Reject | DropNewest | DropOldest
  maxAttempts     a nacked envelope is retried, then dead-lettered
  queue           bounded FIFO
  consumers       the stage's handlers

Worker pool
  one pool shared by every stage
  per-stage permit count == that stage's concurrency limit
  schedule(ch)    event-driven: submit a drain task iff work is queued AND a permit is free
  drain(ch)       process up to BATCH envelopes, release the permit, re-schedule if work remains
```

There is **no polling loop.** Publishing schedules a self-rescheduling drain
task, gated by a per-stage concurrency permit. A drain task processes a bounded
batch (`BATCH` — 16 in Rust/Python, 32 in TS, 64 in Java), releases its permit,
and re-schedules itself if the queue is non-empty. Batching amortises the
scheduling cost; the cap keeps one busy stage from holding a worker forever.

### 1.3 The envelope and the routing slip

An **envelope** is the unit of work: a stable `id`, a `to` (the channel it is
currently headed for), optional `sender`, string `headers`, a `payload`, an
`attempts` counter for the current hop, and a **routing slip** (`slip`) — an
ordered list of further stages to visit.

A consumer advances an envelope through a pipeline by leaving routes on the slip
and returning. When a stage acks an envelope, the bus's `completeHop` logic runs:

```
if the slip has a next hop:
    pop it into env.to, reset env.attempts, re-publish
else:
    fire the producer's completion callback (end of itinerary)
```

So `publish(Envelope{to: "ingest", slip: ["transform", "sink"]})` visits
`ingest → transform → sink` and then calls back. Re-publishing an in-flight hop
uses a short blocking timeout (5s) so a full downstream queue does not silently
drop an itinerary mid-flight.

### 1.4 Delivery within a stage

- **PointToPoint** — one consumer handles each envelope, chosen round-robin
  across the stage's consumers.
- **PubSub** — every consumer (Java: every *subscription channel*) handles every
  envelope; the stage acks only if all of them ack.

A consumer returns a boolean: **ack** (true) or **nack** (false). A thrown
exception / panic / rejected promise is caught and treated as a nack — a
misbehaving consumer never takes down a worker.

### 1.5 Admission control and back-pressure

The queue is bounded by `capacity`. When it is full, the stage's back-pressure
policy decides what `publish` / `offer` does:

| policy       | behaviour when the queue is full                          |
|--------------|-----------------------------------------------------------|
| `Block`      | the producer waits (up to the publish timeout) for room   |
| `Reject`     | `publish` returns false immediately                       |
| `DropNewest` | silently discard the envelope being offered               |
| `DropOldest` | evict the head of the queue, enqueue the new envelope     |

`publish` returns a boolean (TS: `Promise<boolean>`) — whether the envelope was
accepted.

### 1.6 Retry and dead-letter

A nacked envelope with `attempts < maxAttempts` is put back at the **head** of
its own queue and retried (immediately — no backoff). Once attempts are
exhausted, or a stage has no consumers, the envelope is **dead-lettered**: routed
to the source stage's dead-letter channel if one was registered
(`setDeadLetterChannel`), and its completion callback is dropped.

### 1.7 Metrics

Per stage: `depth`, `enqueued`, `delivered`, `nacked`, `dropped`,
`deadLettered` (TS also `inFlight`). Plain counters. `bus.stats()` /
`bus.getStats()` returns a snapshot. SEDA's point is that you measure stages so
you *can* tune them; the tuner that would consume these numbers is not built
(see §3).

### 1.8 Lifecycle

- **start** — the bus starts running and accepting. (Rust/TS start on
  construction; Python via `start()` or the context manager; Java via
  `start(Properties)`.)
- **pause / resume** — stop and resume accepting `publish`es; in-flight work
  finishes either way.
- **shutdown(timeout)** — stop accepting, wait until every queue is drained (or
  the timeout elapses), then stop the pool. Returns whether it drained fully.
- **shutdownNow** — stop the pool without waiting (Java: `shutdown()` vs
  `gracefulShutdown()`).

### 1.9 Concurrency model (shared invariants)

- An envelope is owned by exactly one stage at a time; per-envelope state
  (`slip`, `attempts`) needs no synchronisation.
- The channel registry is safe for concurrent registration and lookup; the hot
  path (publish, drain) avoids global locks — per-stage queue lock plus a
  per-stage permit count.
- Completion callbacks are held by the **bus**, keyed by envelope id, not by the
  channel — a routing slip crosses many channels but has one callback.

---

## 2. Differences between the seven implementations

The design above is common. What differs is dictated by each language's
concurrency model, type system, and ecosystem.

### 2.1 At a glance

|                                            | Java                                                                          | Rust                                                            | Python                                                             | TypeScript                                                              | C++                                                         | C#                                                                              | Go                                                                                   |
|--------------------------------------------|-------------------------------------------------------------------------------|-----------------------------------------------------------------|--------------------------------------------------------------------|-------------------------------------------------------------------------|-------------------------------------------------------------|---------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| Version                                    | `1.3.1`                                                                       | `0.4.0`                                                         | `0.2.0`                                                            | `0.2.0`                                                                 | `0.1.0`                                                     | `0.1.0`                                                                         | `0.1.0`                                                                              |
| Worker pool                                | `Executors.newFixedThreadPool`, daemon threads                                | hand-rolled `Pool` over `mpsc` + OS threads                     | `ThreadPoolExecutor`                                               | the **event loop** (inline) / a `Worker` pool (worker stages)           | hand-rolled `Pool` over a guarded `deque` + OS threads      | the shared, process-wide `ThreadPool` (no owned pool)                           | **none** — a bare `go` goroutine per drain, bounded by a buffered-channel semaphore  |
| True stage parallelism                     | yes (platform threads)                                                        | yes (OS threads)                                                | only under free-threaded CPython (PEP 703); GIL builds serialise   | only for `worker`-configured stages                                     | yes (OS threads)                                            | yes (OS threads, via the CLR's pool)                                            | yes (the Go runtime's M:N scheduler)                                                 |
| Dependencies                               | `ra-common`                                                                   | `ra-common`, `log`                                              | `ra-common`                                                        | `@resolvingarchitecture/ra-common`                                      | `ra-common-cpp`                                             | `ra-common-cs`                                                                  | `ra-common-go`                                                                       |
| Envelope source                            | `ra.common.Envelope` (shared with `service-bus`, `1m5`)                       | `ra_common::Envelope` (rewired at `0.4.0`)                      | `ra_common.Envelope` (rewired at `0.2.0`)                          | `ra-common`'s `Envelope` (rewired at `0.2.0`)                           | `ra::common::Envelope` (aliased)                            | `Ra.Common.Envelope`                                                            | `messaging.Envelope` (aliased as `sedabus.Envelope`)                                 |
| Routing slip                               | `ra.common.DynamicRoutingSlip` — a **LIFO stack** of `Route`                  | `ra_common::DynamicRoutingSlip` — **LIFO** (rewired at `0.4.0`) | `ra_common.DynamicRoutingSlip` — **LIFO** (rewired at `0.2.0`)     | `DynamicRoutingSlip` — **LIFO** (rewired at `0.2.0`)                    | `ra::common::route::DynamicRoutingSlip` — **LIFO**          | `Ra.Common.Routing.DynamicRoutingSlip` — **LIFO**                               | `route.DynamicRoutingSlip` — **LIFO** (append/pop at a slice's end, not front)       |
| Envelope id                                | `nanos-seq` hex                                                               | `Uuid::new_v4()`                                                | `uuid4().hex`                                                      | `crypto.randomUUID()`                                                   | `nanos-seq` hex                                             | `Guid.NewGuid()`                                                                | `util.RandomAlphanumeric(32)`                                                        |
| Guaranteed delivery / persistence          | **yes** — `AtLeastOnce` / `ExactlyOnce`, on-disk store, ordered replay, dedup | no (in-memory only)                                             | no                                                                 | no                                                                      | no (in-memory only)                                         | no (in-memory only)                                                             | no (in-memory only)                                                                  |
| Datatype channels (per-stage type filter)  | **yes**                                                                       | no                                                              | no                                                                 | no                                                                      | no                                                          | no                                                                              | no                                                                                   |
| Pub/sub shape                              | fan-out to registered **subscription channels**                               | fan-out to the stage's own **consumers**                        | fan-out to the stage's own **consumers**                           | fan-out to the stage's own **consumers**; forbidden on worker stages    | fan-out to the stage's own **consumers**                    | fan-out to the stage's own **consumers**                                        | fan-out to the stage's own **consumers**                                             |
| `BATCH`                                    | 64                                                                            | 16                                                              | 16                                                                 | 32                                                                      | 16                                                          | 16                                                                              | 16                                                                                   |
| Config                                     | `Properties` + `ra-sedabus.config` from classpath                             | builder (`ChannelConfig::default().capacity(..)`)               | kwargs on `bus.channel(...)`                                       | options object                                                          | fluent builder (`ChannelConfig{}.Capacity(..)`)             | `record` + `with`-based fluent builder (`new ChannelConfig().WithCapacity(..)`) | plain struct + value-receiver fluent builder (`NewChannelConfig().WithCapacity(..)`) |
| Consumer signature                         | `MessageConsumer.receive(Envelope) -> boolean`                                | `Fn(&mut Envelope) -> bool`                                     | `Callable[[Envelope], bool]` or `Consumer` protocol                | `(env) => boolean \| void \| Promise<boolean \| void>` (async allowed)  | `std::function<bool(Envelope&)>`                            | `delegate bool Consumer(Envelope)`                                              | `type Consumer func(*Envelope) bool`                                                 |
| Back-pressure default                      | (n/a — `ArrayBlockingQueue.offer`, i.e. `Reject`)                             | `Block`                                                         | `Block`                                                            | `Block`                                                                 | `Block`                                                     | `Block`                                                                         | `Block`                                                                              |
| Pull model                                 | `channel.receive()` / `receive(timeout)` / `poll()`                           | no                                                              | no                                                                 | no                                                                      | no                                                          | no                                                                              | no                                                                                   |

### 2.2 Java — `seda-bus-java`

The original, and the one embedded in `service-bus` / `1m5`. Its distinguishing
features:

- **`ra-common` integration.** The envelope, `Client` callback, `ServiceLevel`,
  and `MessageChannel` / `MessageConsumer` / `MessageBus` interfaces all come
  from `ra-common`, so the bus is a drop-in `MessageBus` for the wider stack.
- **The routing slip is a LIFO stack** (`DynamicRoutingSlip`). Producers *push*
  routes and call `ratchet()`; `SEDABus.completed(e)` pops. The other three use a
  FIFO list. Same itinerary concept, opposite data structure and API.
- **Guaranteed delivery.** `ServiceLevel` per channel (or per envelope, which
  overrides): `AtMostOnce` (in-memory), `AtLeastOnce` (envelope written to the
  channel's on-disk store via atomic temp-file + `ATOMIC_MOVE` before `send`
  returns; removed on ack; replayed in filename-time order by
  `sendUnprocessed()`), `ExactlyOnce` (as `AtLeastOnce` plus a bounded
  `LinkedHashMap` of delivered ids — 100 000 — so replay skips duplicates).
  "ExactlyOnce" means *processing* effectively once, not a distributed
  two-phase commit.
- **Datatype channels.** A channel may carry a `Class` filter; `send` rejects
  envelopes whose content is not assignable to it.
- **Pub/sub is channel-to-channel.** A `pubSub` channel fans each envelope out to
  registered *subscription channels* (via `bus.publish` of a copy, so each
  subscriber stage is independently scheduled), not to a consumer list on the
  same stage.
- **Pull model.** `receive()`, `receive(timeout)`, `poll()` on the channel for
  callers that want to drive delivery themselves.
- **Config via `Properties`** loaded from `ra-sedabus.config` on the classpath:
  `ra.sedabus.pool.max` (int, or `Platform` for `2 × cores`),
  `ra.sedabus.channel.locationBase` for the persistence root.
- Concurrency permits are `java.util.concurrent.Semaphore`s, one per channel,
  held by `WorkerThreadPool`. `BATCH` is 64.
- History (see `CHANGELOG.md`): `1.3.0` replaced a 100 ms scan loop (which
  capped throughput at ~10 msg/s/channel) with the event-driven drain, added
  retry/dead-letter and persistence, and fixed a batch of concurrency bugs.
  `1.3.1` lowered the compile target to Java 11 for downstream consumers.

### 2.3 Rust — `seda-bus-rust`

The cleanest expression of the "one real shared thread pool" model.

- **Depends on `ra-common`; carries `ra_common::Envelope`**, same posture as
  every other port — `make_envelope`/`target_service`/`envelope_payload`/
  `set_payload` mirror the other ports' helpers. Per-hop `attempts` live on
  the channel (`Channel::bump_attempt`/`clear_attempt`), keyed by envelope
  id, since `ra_common::Envelope` has none; that map is only touched when a
  stage's `max_attempts > 1` — the default single-attempt case never pays
  for the lock, since the attempt count can't change the outcome when
  there's only one attempt. `log` is the only other dependency. The pool
  (`pool.rs`) is hand-rolled: OS threads parked on a shared
  `Mutex<Receiver<Job>>`, `Job::Run` / `Job::Stop`, `catch_unwind` around
  every job so a panicking consumer cannot kill a worker.
- `Bus` is `Arc<Inner>` and `#[derive(Clone)]` — cheap to hand to consumers and
  callbacks, which is how a consumer re-publishes.
- **Permits are a hand-rolled atomic CAS loop** (`AtomicUsize` +
  `compare_exchange_weak`), not a semaphore type.
- The queue is `Mutex<VecDeque<Envelope>>` + a `Condvar` (`not_full`) for
  `Block` back-pressure, with `wait_timeout` honouring the publish deadline.
- `Consumer` is a trait, blanket-implemented for `Fn(&mut Envelope) -> bool +
  Send + Sync`, so closures work directly.
- `Bus::new(0)` sizes the pool to `available_parallelism()`.
- `ChannelConfig` is a `Copy` builder; `publish(env, Option<Duration>)` takes the
  back-pressure timeout as an argument rather than a channel setting.
- No persistence, no datatype filter, no pull model. `BATCH` is 16.
- Status: pre-1.0, working core, tested (`tests/bus.rs`, `examples/pipeline.rs`).

### 2.4 Python — `seda-bus-python`

Exists specifically to demonstrate that the SEDA model finally pays off on
**free-threaded CPython** (PEP 703; experimental in 3.13, better in 3.14).

- Under the GIL, many CPU-bound stages sharing a thread pool is pointless —
  threads do not run bytecode in parallel. The README carries end-to-end
  measurements: 24 CPU-bound envelopes through one stage scale ~2.6× across a
  12-core pool on `python3.14t` (`PYTHON_GIL=0`), after a ~1.3× single-thread
  tax, whereas under the GIL adding workers makes it *slower*.
- Pool is `concurrent.futures.ThreadPoolExecutor`. Permits are a
  `threading.BoundedSemaphore` per channel; the queue is a `collections.deque`
  guarded by a `threading.Condition` (`not_full`).
- Consumers are a `Consumer` `Protocol` (`runtime_checkable`) or any callable;
  wrapped internally.
- Ergonomics: `bus.channel(name, capacity=..., concurrency=..., delivery=...,
  backpressure=..., max_attempts=...)`, `Delivery` / `Backpressure` are `Enum`s,
  `SEDABus` is a context manager (`__enter__` starts, `__exit__` shuts down).
- **Carries `ra_common.Envelope`, not its own struct, as of the `0.2.0`
  rewire (2026-09-10, user-requested).** `seda_bus.envelope` re-exports it and
  adds `make_envelope`/`target_service` ergonomic helpers over the richer
  type's `DynamicRoutingSlip` (LIFO) routing. Per-hop `attempts` moved off the
  envelope onto `Channel._attempts`, keyed by id, since `ra_common.Envelope`
  has none. `pyproject.toml` now depends on `ra-common`.
- `start()` is explicit (or via the context manager) — unlike Rust/TS which
  start on construction.
- No persistence, no datatype filter, no pull model. `BATCH` is 16. Only
  runtime dependency is `ra-common`; `tests/` is pytest, `bench/` holds the
  raw-interpreter microbenchmarks.

### 2.5 TypeScript — `seda-bus-ts`

The odd one out: Node runs application code on **one thread** (the event loop).
The bus adapts rather than pretends otherwise. (`seda-bus-ts/DESIGN.md` has the
full treatment.)

- **Two transports** behind a `StageTransport` interface:
  - **`InlineTransport`** (default) — consumers are functions on the event loop.
    The "shared pool" is the event loop itself; a stage's `concurrency` is the
    number of `drain` turns that may be interleaved while awaiting I/O. This is
    the right model for I/O-bound stages (the overwhelming majority of Node
    work) — it makes the bus a structured, staged alternative to `p-queue` /
    `bottleneck`.
  - **`WorkerTransport`** (`worker` channel option) — the stage handler is a
    **module** (not a closure — closures cannot cross a thread boundary), run
    across a fixed pool of `Worker` threads. This gives real parallelism for
    CPU-bound stages. The envelope crosses by structured clone; `payload` /
    `headers` / `slip` are copied back; queue, back-pressure, retry, and metrics
    all stay on the main thread. Pub/sub is forbidden on worker stages.
- **Everything is async.** `publish` returns `Promise<boolean>`; consumers may
  return a promise; `shutdown` is async. `drain` turns are `async` and not
  awaited by the scheduler (`void this.drain(ch)`).
- Because JS is single-threaded, the `inFlight` / `globalInFlight` counters need
  **no locks** — the scheduler (`pump`) is a plain `while` loop.
- **Bus-wide concurrency cap** (`SedaBusOptions.concurrency`) in addition to
  per-stage limits — `pump` also checks `globalInFlight < globalLimit`.
- `Block` back-pressure parks the publisher in a `waiters` list; supports a
  `timeoutMs` **and** an `AbortSignal` to abandon the wait.
- **Carries `@resolvingarchitecture/ra-common`'s `Envelope`, not its own, as
  of the `0.2.0` rewire (2026-09-10, user-requested).** `envelope.ts`
  re-exports it and adds `makeEnvelope`/`targetService` helpers over the
  same `DynamicRoutingSlip` (LIFO) routing as Java/Python. Per-hop attempts
  live on the channel, not the envelope. `WorkerTransport` still exchanges
  the envelope as JSON across the thread boundary (the harness rehydrates it
  back into an `Envelope` on the other side).
- ESM-only, `NodeNext` resolution; one runtime dep (`ra-common`); tests use
  `node:test` via `tsx`. `BATCH` is 32.

### 2.6 C++ — `seda-bus-cpp`

Header-only C++20, following `seda-bus-rust`'s concurrency model almost
mechanically — real OS threads, no garbage collector, so the shape translates
directly. (`seda-bus-cpp/DESIGN.md` has the full treatment, including a note
on the initial build getting the envelope-source call wrong.)

- **Header-only**, matching [`ra-common-cpp`](../../common/ra-common-cpp/)'s
  convention — no separate compilation step for the library itself.
- **Depends on `ra-common-cpp`; carries `ra::common::Envelope`**, aliased as
  `ra::seda_bus::Envelope` — same posture as every other port.
  `MakeEnvelope`/`TargetService` mirror
  `make_envelope`/`target_service`; per-hop `attempts` live on the channel
  (`Channel::BumpAttempt`/`ClearAttempt`), keyed by envelope id, since
  `ra::common::Envelope` has none.
- `Pool` (`pool.hpp`) is a hand-rolled fixed-size thread pool: a
  `std::deque<std::function<void()>>` job queue guarded by
  `std::mutex`/`std::condition_variable`, standing in for Rust's `mpsc`
  channel (C++ has no standard MPMC queue). `Join()` lets every already-queued
  job run before joining threads.
- **Permits are the same atomic-CAS loop as Rust** — `std::atomic<size_t>` +
  `compare_exchange_weak`, not a semaphore type.
- `Channel::Offer` under `Backpressure::Block` uses
  `std::condition_variable::wait_until(deadline)`, re-checked by the enclosing
  `while` loop exactly like Rust's `wait_timeout` + manual re-check.
- **`std::shared_mutex`** for the channel registry and each stage's consumer
  list (read far more than written); plain `std::mutex` for the DLQ map and
  completion-callback map — matching Rust's `RwLock` vs `Mutex` split.
- **`Bus` wraps a `std::shared_ptr<detail::Impl>`**, mirroring Rust's
  `#[derive(Clone)] struct Bus(Arc<Inner>)` — copies are cheap and share state.
- **`Consumer` is `std::function<bool(Envelope&)>`**, the direct analogue of
  Rust's blanket `impl<F: Fn(&mut Envelope) -> bool> Consumer for F` — lambdas
  subscribe directly, no adapter class.
- A thrown exception from a consumer is caught and treated as a nack
  (`detail::SafeReceive`), exactly like Rust's `catch_unwind`.
- No persistence, no datatype filter, no pull model. `BATCH` is 16, matching
  Rust and Python.

### 2.7 C# — `seda-bus-cs`

C# / .NET 8, following `seda-bus-java`'s concurrency model instead of Rust's —
.NET, like the JVM, ships a real built-in thread pool and semaphore, so
hand-rolling either (as Rust/C++ must) would be fighting the platform, not
matching it. Envelope choice follows Python/TS/C++. (`seda-bus-cs/DESIGN.md`
has the full treatment.)

- **Depends on `ra-common-cs`; carries `Ra.Common.Envelope`** directly (no
  alias type — `Envelope` is `sealed`, so it can't be subclassed, and C# has
  no first-class module-level re-export the way TS's `export { Envelope }`
  does; consumers just `using Ra.Common;`). `EnvelopeHelpers.MakeEnvelope`/
  `TargetService` mirror `make_envelope`/`target_service`; per-hop
  `Attempts` live on the channel (`Channel.BumpAttempt`/`ClearAttempt`, a
  `ConcurrentDictionary<string, int>`), since `Ra.Common.Envelope` has none.
- **No owned worker pool — the shared, process-wide .NET `ThreadPool` drains
  every stage.** `Bus`'s constructor raises `ThreadPool.SetMinThreads` to a
  floor (advisory, not a fixed size) so a burst of work isn't stalled behind
  the pool's default gradual thread-injection rate. This is the port's one
  real structural divergence from the shared design's "one shared worker
  pool" (§1.2, §1.9) — here it's shared with the whole process, not scoped
  to one `Bus`.
  - **Consequence:** `Shutdown` can't `Join()` a pool it doesn't own, so an
    explicit `_inFlight` counter (`Interlocked.Increment`/`Decrement` around
    every schedule/drain cycle) tracks queued-but-not-yet-finished work;
    `AwaitDrain` waits for `_inFlight == 0` *and* every channel's depth `==
    0`, not depth alone — depth alone is what Rust/C++/Java's shutdown checks
    before `pool.join()`/`awaitTermination()` actually blocks for completion,
    which the shared pool here cannot do for you.
- **`SemaphoreSlim`** for per-channel concurrency permits, matching Java's
  `java.util.concurrent.Semaphore` — not a hand-rolled atomic-CAS loop.
- **`Monitor.Wait`/`Monitor.Pulse`** on a plain `lock` object as the
  condition-variable equivalent for the per-stage queue (a `LinkedList<Envelope>`,
  needed for O(1) head-insert on retry-requeue).
- **`ConcurrentDictionary`** for the channel registry, DLQ map, callback map,
  and per-channel attempts map — no manual locking needed, a genuine
  simplification over the C++/Rust ports' guarded maps.
  **`ReaderWriterLockSlim`** for each stage's consumer list, the one place a
  bare concurrent collection doesn't fit.
- **`ChannelConfig`/`Stats` are `record`s** with `init`-only properties and
  `With*` fluent methods built on `with` expressions — the C# idiom for
  "immutable value, fluent modified copy."
- **`Bus` is a plain class, no `Arc`/`shared_ptr`-equivalent wrapper** — the
  CLR's GC already keeps it alive for as long as a `ThreadPool` work item's
  closure references it.
- No persistence, no datatype filter, no pull model. `BATCH` is 16, matching
  Rust/Python/C++.

### 2.8 Go — `seda-bus-go`

Needs neither a hand-rolled pool (Rust, C++) nor a borrowed built-in one
(Java, C#) — Go's own primitives are a close enough fit to the SEDA shape
that no pool abstraction was needed at all. Envelope choice follows
Python/TS/C++/C#. (`seda-bus-go/DESIGN.md` has the full treatment.)

- **Depends on `ra-common-go`; carries `messaging.Envelope`**, aliased via a
  genuine Go type alias, `type Envelope = messaging.Envelope` (same type,
  not a wrapper) — the closest of any port to TS's `export { Envelope }`
  re-export, since Go's alias declarations are a real language feature for
  exactly this. `MakeEnvelope`/`TargetService` mirror `make_envelope`/
  `target_service`; per-hop attempts live on the channel (`map[string]int`
  behind a `sync.Mutex`), since `messaging.Envelope` has none.
  - `ra-common-go`'s `DynamicRoutingSlip` is LIFO via append/pop at a
    **slice's end**, not the front-insert the other ports use (a documented
    choice in `ra-common-go` itself — O(1) at a slice's natural end).
    Still LIFO, so `MakeEnvelope`'s push order is unchanged.
  - Go has no default/named parameters, so the optional `slip`/`sender`/
    `headers` become the functional-options pattern
    (`MakeEnvelope(to, payload, WithSlip(...), WithSender(...))`) rather
    than positional nils.
  - `payload` is `any`, not a JSON-node wrapper type — `messaging.Envelope`
    already stores content as `map[string]any` with no marshal round-trip on
    a plain `Put`/`Get`, so this is the simplest payload story of any port.
- **No worker pool, hand-rolled or borrowed — each scheduled drain is a
  bare goroutine** (`go bus.drain(ch)`). This is the port's biggest
  structural departure from §1.2/§1.9's "one shared worker pool": goroutines
  are cheap enough (a few KB stack, M:N-scheduled onto OS threads by the Go
  runtime) that pooling them would fight the language rather than match it.
  A bus-wide buffered channel used as a counting semaphore
  (`busPermits := make(chan struct{}, workers)`) still bounds how many drain
  goroutines run concurrently, playing the role a sized pool plays
  elsewhere, on top of each stage's own per-channel semaphore.
  - **Consequence, same as `seda-bus-cs`:** nothing here is a pool
    `Shutdown` can join. An `atomic.Int64` in-flight counter, incremented
    before every `go bus.drain(ch)` and decremented when it returns, lets
    `awaitDrain` wait for zero in-flight *and* zero depth on every channel,
    not depth alone.
- **Buffered channels as counting semaphores** for both per-channel and
  bus-wide concurrency limits (`select` with a `default` case for
  non-blocking try-acquire) — the standard Go idiom for a semaphore, since
  the language ships no counting-semaphore type in its standard library.
- **`sync.Mutex` + `sync.Cond`** — Go's condition-variable equivalent — for
  the per-stage queue, a `container/list.List` (not a slice: repeatedly
  re-slicing a FIFO from the front never shrinks the backing array, a real
  memory-growth gotcha for a long-running bus; `container/list` gives true
  O(1) push/pop at both ends, the Go stdlib's `LinkedList<T>` equivalent).
  `sync.Cond` has no built-in timeout, so a `time.AfterFunc` timer
  `Broadcast()`s on expiry and the waiter re-checks its own deadline on
  every wake — the same pattern as the other ports' `wait_until`/
  `Monitor.Wait(timeout)`.
- **`*time.Duration`, not `context.Context`, for `Publish`'s optional
  timeout** — a deliberate choice, not an oversight; see
  `seda-bus-go/DESIGN.md` for why accepting a context without honouring its
  cancellation would violate Go convention, and why wiring that into a
  `sync.Cond`-based wait was set aside as unverified complexity no test in
  this port exercises.
- No persistence, no datatype filter, no pull model. `BATCH` is 16, matching
  Rust/Python/C++/C#. Verified clean under `go test -race` (five repeated
  runs) — the only port in this family with an actually clean sanitizer run
  behind it; `seda-bus-cpp`'s ThreadSanitizer attempt could not be trusted in
  its sandbox.

---

## 3. What none of them implement: the adaptive controller

SEDA's original design included a **controller** that watched per-stage latency
and queue depth at runtime and re-tuned each stage's thread allocation and shed
load automatically. In all seven implementations every setting is **static
configuration** — you pick each stage's capacity and concurrency up front.

The per-stage metrics (§1.7) exist precisely so a controller *could* consume
them, and this is the reason the model is worth revisiting now that
free-threaded Python and cheap threads make staged concurrency matter again —
but a **full closed-loop thread-pool resizer is probably not worth building**,
based on both the production record and this family's own retrospective:

- **No production precedent.** No major broker (Kafka, NATS, RabbitMQ, Pulsar)
  ships one — see [`seda-bus-compare/3RDPARTY.md`](../seda-bus-compare/3RDPARTY.md)
  for the full comparison. Mule, which is explicitly built on SEDA, documents
  that it "does not provide out-of-the-box mechanisms for adjusting the amount
  of threads used during runtime" — it relies on static developer tuning.
- **Oscillation is a real, documented failure mode**, not a hypothetical one:
  the ADAPT-T paper (2019) found naive adaptive thread-pool sizing "keeps
  oscillating within a small range of values without obtaining any performance
  gain" without an added locking/stabilization mechanism; MySQL's own
  hill-climbing thread-pool sizer needs wave-magnitude bounds and plateau
  detection to avoid the same problem.
- **Matt Welsh's own 2010 retrospective** on SEDA criticized strict
  per-stage thread pools as causing excessive context switches and suggested
  grouping stages under shared pools instead — which undermines a per-stage
  controller's premise, and matches what most of these ports already do (one
  shared pool, not one per stage).
- **It's a policy decision, not a library concern.** What to do under load
  depends on the adopter's own SLOs and workload shape; baking a specific
  closed-loop policy into the library forces one answer on every user.

**What's worth building instead**, cheap and without the oscillation risk:

1. Per-stage metrics are already exposed (§1.7) — keep them first-class in
   every port, not just some.
2. A simple, **stateless adaptive admission/shedding policy** — the go-zero
   `adaptiveShedder` pattern is a good model: sliding-window QPS/latency
   tracking, compute a max concurrency, shed when exceeded. On the order of
   100 lines, doesn't resize any pool, and gives the SEDA paper's
   headline "graceful degradation" property without control-theory
   complexity.
3. A **runtime-tunable pool size** (an atomic or a channel a caller can push
   to), so an external orchestrator (Kubernetes HPA, a sidecar, application
   code) can adjust it using the metrics from (1) — adaptation stays outside
   the library, where the policy choice belongs.

The Java `TODO.md` sketches a fuller `2.0` along similar lines: cheap on-path
instrumentation (queue depth, wait time, service time, throughput, nack rate),
admission-time load shedding with optional upstream back-pressure signalling,
and a pluggable controller policy — read that alongside the above, not instead
of it.

Smaller shared gaps: no backoff between retry attempts (immediate re-queue),
strict FIFO per stage (no priority), per-channel `BATCH` is a constant, and
dead-letter stores/files are unbounded.

## 4. Positioning notes

Angles worth using when describing this project externally, not yet polished
into actual marketing copy:

- The portability story: the same design, implemented seven times, gives an
  adopter's architecture room to survive a language change.
- Explicit, visible per-stage admission control is the real point of
  contrast with a broker like Kafka (no admission control) or Storm
  (bang-bang only, see `3RDPARTY.md`) — this design makes each stage's
  backpressure policy a first-class, inspectable setting instead of an
  emergent property of queue depth.
- The credible version of a competitive benchmark isn't "faster than Kafka"
  (unlikely to be true) — it's behavior at overload: does a consumer lag
  silently, does a spout stall, or does the bus shed load at a known stage
  with a flat `p99` elsewhere. That is the SEDA paper's original pitch, and
  `seda-bus-compare`'s capacity-curve benchmark is built to show exactly this.
