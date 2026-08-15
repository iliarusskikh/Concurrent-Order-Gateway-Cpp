#pragma once

// =============================================================================
// publish_event.hpp — What travels matcher → publisher on the event SPSC.
//
// Timestamps (steady_clock) enable Phase 5 latency histograms:
//   queue_wait     = match_started_at - enqueued_at
//   match_duration = matched_at - match_started_at
//   match_to_publish (recorded in publisher) = published_at - matched_at
// =============================================================================

#include "gateway_order.hpp"
#include "lob/types.hpp"

#include <chrono>
#include <utility>

namespace cog {

struct PublishEvent {
  SymbolId symbol_id{0};
  lob::Event event;

  std::chrono::steady_clock::time_point enqueued_at{};
  std::chrono::steady_clock::time_point match_started_at{};
  std::chrono::steady_clock::time_point matched_at{};  // after submit() returns
};

inline PublishEvent make_publish_event(SymbolId symbol, lob::Event ev,
    std::chrono::steady_clock::time_point enqueued_at,
    std::chrono::steady_clock::time_point match_started_at,
    std::chrono::steady_clock::time_point matched_at) {
  PublishEvent pe;
  pe.symbol_id = symbol;
  pe.event = std::move(ev);
  pe.enqueued_at = enqueued_at;
  pe.match_started_at = match_started_at;
  pe.matched_at = matched_at;
  return pe;
}

}  // namespace cog
