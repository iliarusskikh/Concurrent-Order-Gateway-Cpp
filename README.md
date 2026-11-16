# Concurrent Order Gateway (C++)

Multi-threaded **order gateway + matching pipeline**: ingress validates and shards orders onto per-symbol queues; matcher workers each own a book and match; a publisher thread emits fills/events. Focus is concurrency, ownership, and backpressure around an existing matching engine — not another CLOB or market-data feed.

Companion to the [TCP Market Data Simulator](https://github.com/iliarusskikh/TCP-Market-Data-Simulator-Cpp) (I/O + framing) and the [Limit Order Book](https://github.com/iliarusskikh/Limit-Order-Book-and-Matching-Engine-Cpp) (single-threaded matching). **No third-party libraries** beyond the C++ standard library (CMake to build).

**Scope (v1):** CLI / file ingress. Bounded SPSC queues, shard-by-symbol, mutex + condition variables. LOB reused as a black box. No TCP, no lock-free book. v1 complete (phases 0–5).

| Doc | Purpose |
|-----|---------|
| [`theory/index.html`](theory/index.html) | Knowledge notes for concepts used in this tree |
| [`LICENSE`](LICENSE) | MIT |

---

## What it does

Orders enter from a scenario file (`NEW …` / optional `SYM <id>`). The gateway routes each order by `symbol_id` onto that symbol’s ingress queue. A dedicated matcher thread owns that symbol’s `MatchingEngine`, matches, and pushes events onto a per-shard event queue. A publisher thread drains those events to stdout / an in-memory log and records latency samples.

```text
CLI / scenario file
         │
         ▼
  ┌──────────────┐   SPSC per symbol        ┌──────────────────┐
  │ Gateway      │ ───────────────────────► │ Matcher worker i │──► LOB
  │ validate +   │   ingress[i]             │ owns engine[i]   │
  │ shard        │                          └────────┬─────────┘
  └──────────────┘                                   │ events[i] (SPSC)
                                                     ▼
                                              Publisher → log / stdout
                                              + latency histograms
```

Per-shard **event** queues keep each hop SPSC (sharing one event queue across matchers would be MPSC). The publisher round-robins and waits on `EventWake`. `Shard` holds mutexes → stored as `unique_ptr<Shard>`.

Drop counters: ingress full, event full, unknown symbol.

---

## Scope lock (v1)

| In | Out |
|----|-----|
| Multi-symbol (e.g. 4 shards) | Networking / TCP ingress (optional later) |
| Parse DSL → enqueue by symbol | Lock-free book, NUMA, persistence |
| N matcher threads (1 symbol each) | Amend / FOK / icebergs / UI |
| Bounded SPSC; full queue → drop + count | Global mutex around one shared book |
| Publisher → log / stdout | Full mini-exchange + GUI |
| Latency histograms (queue / match / publish) | CPU pinning, rdtsc, huge pages |
| Unit + concurrency smoke tests | Rewriting LOB matching logic |

Deliberate cuts: TCP already covered by MDS; lock-free ring harder to verify (same API later); shared global book conflicts with shard ownership.

---

## Design decisions

| Topic | Choice | Why |
|-------|--------|-----|
| Concurrency | Shard-by-symbol; one thread owns one book | Keeps LOB single-threaded and correct |
| Queues | Bounded mutex SPSC; per-shard ingress **and** events | SPSC fits the topology; shared event queue would be MPSC |
| Full queue | `try_push` → drop + atomic counter | Makes overload visible |
| LOB | Vendored slim core under `include/lob/` | Standalone build; matching stays a black box |
| Ingress | CLI / scenario file | Concurrency path without sockets |
| Latency | `steady_clock` histograms | Honest in-process stage timings |
| Language | C++17 | Threads, atomics, chrono, templates |

---

## Latency metrics

| Metric | Definition |
|--------|------------|
| `queue_wait` | `match_started_at - enqueued_at` (per order) |
| `match_duration` | `matched_at - match_started_at` (per order) |
| `match_to_publish` | `published_at - matched_at` (per published event) |

Reported at shutdown as p50 / p99 (and min / max).

---

## C++ concepts in this tree

| Concept | Where |
|---------|--------|
| `std::thread` | Matchers + publisher (`multi_symbol_pipeline.hpp`) |
| Mutex + condition variable | `spsc_queue.hpp`, `event_wake.hpp` |
| Atomics | `ingress_done`, `matchers_left`, drop counters |
| Move semantics | `GatewayOrder` / `PublishEvent` through queues |
| `unique_ptr` / RAII | Non-movable shards; `ThreadGuard` join |
| Lambdas | Thread entry, CV stop predicates |
| Templates | `SpscQueue<T, N>` |

Knowledge notes: [`theory/index.html`](theory/index.html) (architecture → concurrency → ownership → latency → testing → code ref).

---

## Project structure

```
├── CMakeLists.txt
├── LICENSE
├── README.md
├── include/          # gateway plumbing + vendored lob/
├── src/main.cpp
├── scenarios/
├── tests/
└── theory/           # HTML knowledge notes
```

## Requirements

- C++17, CMake 3.16+, g++/clang++ (Linux / macOS / WSL)

## Build & run

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build

./build/lob_tests
./build/gateway_tests
./build/e2e_tests
./build/multi_tests
./build/latency_tests
./build/gateway scenarios/multi_symbol_demo.txt 2
```

**Suggested code reading order:** `gateway_order.hpp` → `spsc_queue.hpp` → `thread_guard.hpp` → `matcher_worker.hpp` → `publisher.hpp` → `multi_symbol_pipeline.hpp` → `latency_stats.hpp` → `src/main.cpp`.

---

## Optional next work

1. TCP ingress (reuse MDS framing or a thin length-prefixed DSL)
2. Lock-free SPSC behind the same queue API; compare histograms
3. Object pool for order nodes under load
4. MPSC ingress fan-in (multiple producers into a router)
5. Bounded event-side backpressure toward a slow publisher

---

## Related projects

| Project | Focus |
|---------|--------|
| TCP MDS (C++) | Sockets, framing, `poll`, RAII — single-threaded I/O |
| LOB matcher (C++) | Price-time book, events, golden tests — no threads |
| **This gateway (C++)** | Threads, SPSC, sharding, backpressure, ownership under load |
