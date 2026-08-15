# Concurrent Order Gateway — Plan

**Status:** v1 complete (phases 0–5). Optional follow-ons listed below.

**Scope lock:** CLI / file ingress. Multi-threaded pipeline (gateway → per-symbol SPSC → matcher workers → publisher). LOB reused as a black box. Mutex + CV SPSC. No TCP, no lock-free matching book.

Docs: [README](README.md) · [theory knowledge notes](theory/index.html)

---

## Goal

A concurrent order path that sits between the TCP MDS and the LOB:

- Reuse matching (LOB); do not rebuild the book.
- Skip networking in v1 (MDS already covers framing).
- Practice threads, bounded queues, ownership/moves, backpressure, and measured latency.

---

## Locked decisions (v1)

| Decision | Choice |
|----------|--------|
| Ingress | CLI / scenario file |
| Queue | Bounded SPSC, mutex + condition variable |
| Event path | **Per-shard** event SPSC (not one shared MPSC queue) |
| Ownership | One matcher thread owns one symbol |
| Full queue | `try_push` → drop + atomic counter |
| LOB | Vendored slim copy in `include/lob/` |
| Dialect | C++17 |
| Networking | Out of v1 |

---

## Roadmap

| Phase | Goal | Status |
|-------|------|--------|
| 0 | README, PLAN, `.gitignore`, theory notes | ✅ |
| 1 | Vendored LOB; `GatewayOrder` | ✅ `lob_tests` |
| 2 | `SpscQueue` + `ThreadGuard` | ✅ `gateway_tests` |
| 3 | One-symbol E2E pipeline | ✅ `e2e_tests` |
| 4 | Multi-symbol sharding | ✅ `multi_tests` |
| 5 | Latency histograms + docs | ✅ `latency_tests` |
| 6+ | TCP ingress / lock-free SPSC / object pool | Optional |

---

## What shipped (v1)

- Scenario DSL loader (`NEW …` / optional `SYM <id>`)
- Multi-symbol pipeline: ingress SPSC → matcher → event SPSC → publisher
- Drop counters (ingress full, event full, unknown symbol)
- Latency: `queue_wait`, `match_duration`, `match_to_publish` (p50/p99)
- Tests: LOB goldens, SPSC unit, one-symbol E2E, multi-symbol, latency

### Important design note (event path)

N matchers must not share one event queue — that would be MPSC and break the SPSC contract. Each shard has its own `events` queue; the publisher round-robins and waits on `EventWake`. `Shard` holds mutexes → stored as `unique_ptr<Shard>`.

### Latency metrics

| Metric | Definition |
|--------|------------|
| `queue_wait` | `match_started_at - enqueued_at` (per order) |
| `match_duration` | `matched_at - match_started_at` (per order) |
| `match_to_publish` | `published_at - matched_at` (per published event) |

---

## Out of scope (deliberate)

| Cut | Why |
|-----|-----|
| TCP / sockets in v1 | Already covered by MDS; CLI isolates the concurrency lesson |
| Lock-free ring | Harder to verify; optional later behind the same API |
| Shared mutex on one global book | Conflicts with shard-by-symbol ownership |
| Amend, FOK, stops, icebergs | LOB feature surface, not gateway plumbing |
| Persistence, GUI, FIX, TLS | Outside the learning goal |
| NUMA, pinning, huge pages, rdtsc | Unnecessary for this scale |

---

## Optional next work

1. TCP ingress (reuse MDS framing or a thin length-prefixed DSL)
2. Lock-free SPSC behind the same queue API; compare histograms
3. Object pool for order nodes under load
4. MPSC ingress fan-in (multiple producers into a router)
5. Bounded event-side backpressure toward a slow publisher

---

## Code reading order

| # | File | Topic |
|---|------|--------|
| 1 | `include/gateway_order.hpp` | Queue message |
| 2 | `include/thread_guard.hpp` | RAII join |
| 3 | `include/spsc_queue.hpp` | Bounded SPSC |
| 4 | `include/matcher_worker.hpp` | Pop → submit → push events |
| 5 | `include/publisher.hpp` | Drain events + latency sample |
| 6 | `include/multi_symbol_pipeline.hpp` | Orchestration + shutdown |
| 7 | `include/latency_stats.hpp` | Percentile histograms |
| 8 | `include/lob/matching_engine.hpp` | Vendored match API |
| 9 | `src/main.cpp` | CLI |

---

## Knowledge notes

See [`theory/index.html`](theory/index.html). Notes explain the systems/C++ ideas behind this codebase (sharding, SPSC, sync, ownership, latency, testing) — not a build walkthrough.

Suggested reading:

1. Architecture & decisions  
2. Concurrency (model, queues, sync, backpressure)  
3. Ownership & language (move, RAII, templates, lambdas)  
4. Latency & clocks → Testing  
5. C++ Code Reference while reading headers  
