#pragma once

// =============================================================================
// matcher_worker.hpp — Consumer of ONE ingress SPSC; owner of ONE MatchingEngine.
//
// ROLE
//   This thread is the ONLY code that touches `engine` for its symbol/shard.
//
// PHASE 4 — each matcher has its own outbound event SPSC (still SPSC).
// PHASE 5 — records queue_wait + match_duration into PipelineStats (once/order).
// =============================================================================

#include "event_wake.hpp"
#include "gateway_order.hpp"
#include "latency_stats.hpp"
#include "lob/matching_engine.hpp"
#include "publish_event.hpp"
#include "spsc_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <utility>
#include <vector>

namespace cog {

inline constexpr std::size_t kEventQueueCapacity = 1024;
inline constexpr std::size_t kIngressQueueCapacity = 1024;

template <std::size_t IngressN, std::size_t EventN>
void run_matcher_worker(SpscQueue<GatewayOrder, IngressN>& ingress, SpscQueue<PublishEvent, EventN>& events,
    lob::MatchingEngine& engine, const std::atomic<bool>& ingress_done,
    std::atomic<int>& matchers_left, std::atomic<std::uint64_t>& event_drops,
    EventWake* wake, PipelineStats* stats) {  // nullable: tests that ignore latency may pass nullptr
  auto stop = [&] { return ingress_done.load(std::memory_order_acquire); };

  GatewayOrder go;
  while (ingress.pop_wait(go, stop)) {
    using clock = std::chrono::steady_clock;
    const auto match_started_at = clock::now();
    std::vector<lob::Event> produced = engine.submit(go.request);
    const auto matched_at = clock::now();

    // One sample per order (not per event) so Accept+Trade does not double-count.
    if (stats != nullptr) {
      const auto wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(match_started_at - go.enqueued_at).count();
      const auto match_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(matched_at - match_started_at).count();
      stats->queue_wait.add_ns(wait_ns);
      stats->match_duration.add_ns(match_ns);
    }

    for (auto& ev : produced) {
      PublishEvent pe = make_publish_event(go.symbol_id, std::move(ev), go.enqueued_at, match_started_at,matched_at);

      if (!events.try_push(std::move(pe))) {
        event_drops.fetch_add(1, std::memory_order_relaxed);
      }
      if (wake != nullptr) {
        wake->notify();
      }
    }
  }

  matchers_left.fetch_sub(1, std::memory_order_acq_rel);
  if (wake != nullptr) {
    wake->notify();
  }
  events.wake_all();
}

}  // namespace cog
