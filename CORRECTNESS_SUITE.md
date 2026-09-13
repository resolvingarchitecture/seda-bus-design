# SEDA Bus — Correctness Suite

A fixed, language-agnostic list of behavioral properties every `seda-bus-*`
port's own test suite should verify, in its own native test framework
(JUnit, `cargo test`, `go test`, `pytest`/`unittest`, Jest/Vitest, doctest,
xUnit). Not a benchmark — every test here is pass/fail, not a number.

## Why this exists

`seda-bus-compare`'s benchmark measures throughput and latency under one
workload. It cannot tell an adopter whether a port's backpressure policies
actually work, whether a crashing consumer takes the bus down, whether
`Shutdown` can lie about having drained, or whether a port leaks a resource
every time it's constructed and disposed — the exact kind of gap a
production-readiness audit of all seven ports found in six of them (one
port each, except C# which had two, and Java which was missing an entire
policy). This suite exists so those properties are asserted once and
checked on every change, in every port, the same way — not rediscovered by
audit each time.

Each item below cites the `DESIGN.md` section it verifies. A port passing
all of them is not proof of no bugs — it's proof these specific,
previously-real bugs (or their direct siblings) don't currently exist.

## The checklist

### C1 — Backpressure policy correctness (DESIGN.md §1.5)

For **each** of the four policies, on a channel with a small fixed capacity
and a gated/slow consumer (so the queue provably fills):

- **Block**: a producer that keeps publishing past capacity is not
  rejected — every publish eventually returns/resolves `true` once room
  frees up. Verify with more publishes than capacity, bounded by an
  explicit timeout on the producer's own thread/task (never a bare
  join/await with no deadline) — a regression here should fail the test
  loudly, not hang the suite.
- **Reject**: publishing past capacity returns `false` immediately (no
  waiting), and the `dropped` counter increases by exactly the number of
  rejections.
- **DropNewest**: identical observable contract to Reject from the
  caller's side (returns `false`, doesn't admit) — this is intentional,
  not a bug, and shared by every port; the test exists to confirm the
  policy is actually wired to distinguishable, intentional behavior, not
  silently ignored.
- **DropOldest**: every publish past capacity still returns `true` (the
  newest envelope is always admitted), depth never exceeds capacity, and
  the evicted-oldest envelope is verifiably gone (never delivered).

### C2 — Retry → dead-letter correctness (DESIGN.md §1.6)

A consumer that nacks deterministically until a known attempt count, then
(a) succeeds on the final allowed attempt, and separately (b) never
succeeds:

- (a): delivered exactly once, attempt-tracking state cleared afterward (no
  leak in the per-envelope attempts map).
- (b): dead-lettered after exactly `maxAttempts` tries, routed to the
  configured dead-letter channel if one is set, and never delivered to the
  original consumer again.
- A channel with **no consumers at all** dead-letters immediately rather
  than silently discarding.

### C3 — Consumer failure isolation (DESIGN.md §1.4)

A consumer that throws/panics/rejects on every Nth envelope: the bus
keeps running, the worker/thread/task is not lost, and every other
envelope on that channel — before and after the failure — is still
delivered. This is the one property every port already passed in the
audit; keep it that way.

### C4 — Shutdown accounting (DESIGN.md §1.8)

`shutdown(timeout)` returning `true` (fully drained) must mean every
published envelope was either delivered or dead-lettered — not merely
that a queue *looked* empty at some polling instant while an envelope
was still popped-but-not-yet-acked on a worker. Verify by publishing a
batch with an artificial per-envelope processing delay, calling shutdown
concurrently, and checking `delivered + deadLettered == published` once
it returns — for both the "drained before timeout" and "timeout expired
first" outcomes.

### C5 — Config validation is defined, not silent (DESIGN.md §1.5, §1.9)

Constructing a channel with an invalid value (`capacity <= 0`,
`concurrency <= 0`, `maxAttempts <= 0`) does **one** of two documented
things — fails fast (an exception/error/panic at construction) or clamps
to a documented minimum — and a test pins down which, for this port,
explicitly. Either is acceptable; an *unspecified* result (works
sometimes, misbehaves silently, or is simply untested) is not.

### C6 — No resource leak across repeated lifecycles

Construct-and-fully-shutdown a bus `N` times (`N` ≥ 20) in a loop. Whatever
per-instance resource this port's design allocates externally to normal
GC/refcounting — an OS thread pool, a raised process-wide scheduler floor,
a file handle, a timer — must not grow monotonically across the loop. This
is the exact shape of bug found in `seda-bus-cs` (a permanently-raised
ThreadPool floor); most ports don't have an obvious analog, in which case
this test asserts thread count (or the port's nearest equivalent) returns
to its pre-loop baseline within a small tolerance after the last
shutdown, and exists as a guard against a *future* one.

### C7 — Concurrency correctness under real contention (DESIGN.md §1.9)

Already required in every port before this suite existed — kept here for
completeness, not newly added. Multiple producer threads/tasks publishing
concurrently to one shared, multi-consumer channel deliver every envelope
exactly once (no loss, no duplication). Run repeatedly (locally: a
handful of times; in CI: consider a race detector / sanitizer where the
language has one — `go test -race`, C++ ThreadSanitizer, Rust `loom` or
repeated stress runs, `-Dgc.stress`-equivalent for the GC'd languages).

## Reporting

Each port's own README (or a `TESTING.md` if that reads better for that
port) should state, in one table, which of C1–C7 are covered and by which
test name(s) — so `git grep` isn't required to answer "does this port
actually test its own DropOldest policy." `seda-bus/CORRECTNESS_SUITE.md`
(this file) is the source of truth for *what* C1–C7 mean; each port decides
*how* to express them in its own idiom and test framework.

## Coverage across all seven ports

All seven ports pass C1–C7 as of this writing. See each port's own README
for exact test names.

| Item | Java | Rust | Python | TypeScript | C++ | C# | Go |
|---|---|---|---|---|---|---|---|
| C1 Backpressure (all 4 policies) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| C2 Retry → dead-letter | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| C3 Consumer failure isolation | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| C4 Shutdown accounting | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| C5 Config validation | clamp | clamp | fail-fast | clamp | clamp | fail-fast | clamp |
| C6 Resource lifecycle, measured via | live thread count | owned pool join | live thread count | active handles | `/proc/self/status` thread count (Linux) | ThreadPool floor + thread count | goroutine count |
| C7 Concurrency correctness | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

**C5 is a real, adopter-relevant difference, not a gap**: Python and C#
raise/throw on an invalid `capacity`/`concurrency`/`maxAttempts` at
construction; the other five clamp to the documented minimum instead.
Both are valid choices — this row exists so the choice is visible, not
buried in each port's source.

**Two ports' config objects are public structs/records with settable
fields, independent of their fluent builder's clamping** — C++'s
`ChannelConfig` and C#'s `ChannelConfig` can each be constructed directly,
bypassing the builder. Both validate the effective values again in
`Channel`'s own constructor, the one choke point every construction path
goes through, so a bypassed builder still can't produce an unusable
(e.g. zero-capacity, which would hang `Block` forever) channel.

**Rust's `shutdown(timeout)`, when the timeout elapses before every queue
is empty**, sweeps and accounts every remaining envelope as `dropped`
rather than leaving it queued-but-unreachable once the pool has stopped —
the invariant it guarantees is "every accepted envelope ends up delivered,
dead-lettered, or dropped," including the timeout-exceeded case.

**Java's channel-level `shutdown()`/`gracefulShutdown()`** (on
`SEDAMessageChannel` directly, bypassing `SEDABus`) checks queue depth only,
not in-flight work — `SEDABus.shutdown()`, what every caller actually uses,
is unaffected: `WorkerThreadPool`'s own `awaitTermination` call afterward is
a real barrier. A channel driven standalone, without a bus, would not have
that backstop.
