#pragma once

// =============================================================================
// gateway_order.hpp — What travels on the ingress SPSC queues.
//
// READ THIS FIRST if you are new to the project:
//   The LOB MatchingEngine takes a lob::OrderRequest (id, side, type, price, qty)
//   for ONE instrument. This gateway must also know WHICH symbol the order is for,
//   and WHEN it entered the queue (for latency histograms later).
//
//   So GatewayOrder = routing metadata + LOB request + timestamp.
//   Matcher workers strip it down to lob::OrderRequest when calling submit().
// =============================================================================

#include "lob/types.hpp"

#include <chrono>
#include <cstdint>
#include <utility>

namespace cog {  // Concurrent Order Gateway

// ---------------------------------------------------------------------------
// SymbolId
//   Integer label for an instrument (0 = AAPL demo, 1 = MSFT, …).
//   v1 uses a small fixed set (e.g. 4 symbols). No string tickers on the hot path.
// ---------------------------------------------------------------------------
using SymbolId = std::uint32_t;

// ---------------------------------------------------------------------------
// GatewayOrder
//   Move-friendly message that the gateway thread pushes into a per-symbol
//   SPSC queue. The matching worker for that symbol is the only consumer.
//
//   Why not put symbol_id inside lob::OrderRequest?
//     Keep the vendored LOB untouched. Symbol is a *routing* concern of the
//     gateway, not a matching-rules concern of the book.
// ---------------------------------------------------------------------------
struct GatewayOrder {
  SymbolId symbol_id{0};

  // The actual order the LOB understands (POD: cheap to copy or move).
  lob::OrderRequest request{};

  // Set by the gateway at enqueue time. Matcher / stats use this later to
  // compute queue-wait = match_start - enqueued_at.
  // steady_clock: monotonic, good for intervals (not wall-clock logging).
  std::chrono::steady_clock::time_point enqueued_at{};
};

// ---------------------------------------------------------------------------
// make_gateway_order
//   Helper so call sites stay readable. Stamps enqueued_at = now() by default.
//
//   Example:
//     auto go = make_gateway_order(0, {1, Side::Buy, OrderType::Limit, 100, 10});
// ---------------------------------------------------------------------------
inline GatewayOrder make_gateway_order(SymbolId symbol, lob::OrderRequest req, std::chrono::steady_clock::time_point when = std::chrono::steady_clock::now()) {
  GatewayOrder go;
  go.symbol_id = symbol;
  go.request = std::move(req);  // POD move == copy; habit for later non-POD types
  go.enqueued_at = when;
  return go;
}

}  // namespace cog
