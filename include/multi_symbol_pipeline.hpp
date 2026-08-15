#pragma once

// =============================================================================
// multi_symbol_pipeline.hpp — Phase 4: shard-by-symbol concurrent pipeline.
//
// PICTURE (num_symbols = 4 example)
//
//   gateway thread
//       │  route by symbol_id
//       ├─ try_push → ingress[0] → matcher0 (engine0) → events[0] ─┐
//       ├─ try_push → ingress[1] → matcher1 (engine1) → events[1] ─┤
//       ├─ try_push → ingress[2] → matcher2 (engine2) → events[2] ─┼→ publisher
//       └─ try_push → ingress[3] → matcher3 (engine3) → events[3] ─┘
//
// INVARIANTS
//   - Matcher i is the only thread that touches engines[i]
//   - Ingress[i] is SPSC: gateway → matcher i
//   - Events[i]  is SPSC: matcher i → publisher
//   - symbol_id >= num_symbols → counted drop (unknown_symbol_drops)
//
// SHUTDOWN
//   1. Enqueue batch
//   2. ingress_done=true; wake all ingress queues
//   3. Each matcher drains, decrements matchers_left, notifies EventWake
//   4. Publisher drains all event queues until matchers_left==0
//   5. Join all threads
// =============================================================================

#include "event_log.hpp"
#include "event_wake.hpp"
#include "gateway_order.hpp"
#include "latency_stats.hpp"
#include "lob/matching_engine.hpp"
#include "matcher_worker.hpp"
#include "publisher.hpp"
#include "publish_event.hpp"
#include "spsc_queue.hpp"
#include "thread_guard.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace cog {

// ---------------------------------------------------------------------------
// Shard
//   Everything owned by one symbol's matching worker.
// ---------------------------------------------------------------------------
struct Shard {
  SpscQueue<GatewayOrder, kIngressQueueCapacity> ingress;
  SpscQueue<PublishEvent, kEventQueueCapacity> events;
  lob::MatchingEngine engine;
};

// ---------------------------------------------------------------------------
// MultiSymbolPipeline
// ---------------------------------------------------------------------------
class MultiSymbolPipeline {
public:
  // num_symbols: shards 0 .. num_symbols-1 (must be >= 1).
  explicit MultiSymbolPipeline(std::size_t num_symbols, bool echo_stdout = true)
      : num_symbols_(num_symbols), echo_stdout_(echo_stdout) {
    if (num_symbols_ < 1) {
      throw std::invalid_argument("num_symbols must be >= 1");
    }
    // unique_ptr: SpscQueue contains mutex/CV → Shard is not movable/copyable,
    // so it cannot live directly in a resizing std::vector.
    shards_.reserve(num_symbols_);
    for (std::size_t i = 0; i < num_symbols_; ++i) {
      shards_.push_back(std::make_unique<Shard>());
    }
  }

  std::size_t num_symbols() const { return num_symbols_; }

  EventLog& run(const std::vector<GatewayOrder>& orders) {
    log_.clear();
    stats_.clear();
    ingress_drops_.store(0);
    event_drops_.store(0);
    unknown_symbol_drops_.store(0);
    ingress_done_.store(false);

    // Reset shard state for a clean batch.
    for (auto& shard : shards_) {
      shard->ingress.clear();
      shard->events.clear();
      shard->engine = lob::MatchingEngine{};
    }

    matchers_left_.store(static_cast<int>(num_symbols_), std::memory_order_release);

    std::vector<SpscQueue<PublishEvent, kEventQueueCapacity>*> event_ptrs;
    event_ptrs.reserve(num_symbols_);
    for (auto& shard : shards_) {
      event_ptrs.push_back(&shard->events);
    }

    ThreadGuard publisher{std::thread([this, event_ptrs] {
      run_publisher_multi(event_ptrs, matchers_left_, wake_, log_, echo_stdout_, &stats_);
    })};

    std::vector<std::unique_ptr<ThreadGuard>> matchers;
    matchers.reserve(num_symbols_);
    for (std::size_t i = 0; i < num_symbols_; ++i) {
      matchers.push_back(std::make_unique<ThreadGuard>(std::thread([this, i] {
        run_matcher_worker(shards_[i]->ingress, shards_[i]->events, shards_[i]->engine, ingress_done_, matchers_left_, event_drops_, &wake_, &stats_);
      })));
    }

    for (const GatewayOrder& src : orders) {
      if (static_cast<std::size_t>(src.symbol_id) >= num_symbols_) {
        unknown_symbol_drops_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      GatewayOrder go = src;
      go.enqueued_at = std::chrono::steady_clock::now();

      Shard& shard = *shards_[go.symbol_id];
      if (!shard.ingress.try_push(std::move(go))) {
        ingress_drops_.fetch_add(1, std::memory_order_relaxed);
      }
    }

    ingress_done_.store(true, std::memory_order_release);
    for (auto& shard : shards_) {
      shard->ingress.wake_all();
    }
    wake_.notify();

    for (auto& m : matchers) {
      m->join();
    }
    publisher.join();

    return log_;
  }

  std::uint64_t ingress_drops() const {
    return ingress_drops_.load(std::memory_order_relaxed);
  }
  std::uint64_t event_drops() const {
    return event_drops_.load(std::memory_order_relaxed);
  }
  std::uint64_t unknown_symbol_drops() const {
    return unknown_symbol_drops_.load(std::memory_order_relaxed);
  }

  const lob::MatchingEngine& engine(SymbolId symbol) const {
    return shards_.at(symbol)->engine;
  }

  EventLog& log() { return log_; }
  const PipelineStats& stats() const { return stats_; }
  PipelineStats& stats() { return stats_; }

private:
  std::size_t num_symbols_;
  bool echo_stdout_;

  std::vector<std::unique_ptr<Shard>> shards_;
  EventLog log_;
  EventWake wake_;
  PipelineStats stats_;

  std::atomic<bool> ingress_done_{false};
  std::atomic<int> matchers_left_{0};
  std::atomic<std::uint64_t> ingress_drops_{0};
  std::atomic<std::uint64_t> event_drops_{0};
  std::atomic<std::uint64_t> unknown_symbol_drops_{0};
};

}  // namespace cog
