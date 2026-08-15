#pragma once

// =============================================================================
// publisher.hpp — Drains per-shard event SPSC queues into EventLog (+ latency).
//
// PHASE 5: on each event, record match_to_publish = now - matched_at.
// =============================================================================

#include "event_log.hpp"
#include "event_wake.hpp"
#include "latency_stats.hpp"
#include "lob/event_format.hpp"
#include "publish_event.hpp"
#include "spsc_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <string>
#include <vector>

namespace cog {

namespace detail {

inline std::string format_publish_line(const PublishEvent& pe) {
  return "sym=" + std::to_string(pe.symbol_id) + " " + lob::format_event(pe.event);
}

inline void emit(const PublishEvent& pe, EventLog& log, bool echo_stdout, PipelineStats* stats) {
  if (stats != nullptr) {
    const auto published_at = std::chrono::steady_clock::now();
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(published_at - pe.matched_at).count();
    stats->match_to_publish.add_ns(ns);
  }

  const std::string line = format_publish_line(pe);
  log.append(line);
  if (echo_stdout) {
    std::cout << line << '\n';
  }
}

}  // namespace detail

template <std::size_t EventN>
void run_publisher_multi(const std::vector<SpscQueue<PublishEvent, EventN>*>& event_queues, std::atomic<int>& matchers_left, EventWake& wake, EventLog& log, bool echo_stdout, PipelineStats* stats) {
  auto any_pending = [&] {
    for (auto* q : event_queues) {
      if (q != nullptr && !q->empty()) return true;
    }
    return false;
  };

  while (true) {
    bool progressed = false;
    for (auto* q : event_queues) {
      if (q == nullptr) continue;
      PublishEvent pe;
      while (q->try_pop(pe)) {
        detail::emit(pe, log, echo_stdout, stats);
        progressed = true;
      }
    }

    if (matchers_left.load(std::memory_order_acquire) == 0) {
      for (auto* q : event_queues) {
        if (q == nullptr) continue;
        PublishEvent pe;
        while (q->try_pop(pe)) {
          detail::emit(pe, log, echo_stdout, stats);
        }
      }
      break;
    }

    if (!progressed) {
      wake.wait([&] { return matchers_left.load(std::memory_order_acquire) == 0 || any_pending();});
    }
  }
}

}  // namespace cog
