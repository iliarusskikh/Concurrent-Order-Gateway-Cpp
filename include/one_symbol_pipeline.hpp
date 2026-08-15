#pragma once

// =============================================================================
// one_symbol_pipeline.hpp — Thin Phase-3 wrapper around MultiSymbolPipeline.
//
// Kept so existing e2e tests and docs that say "OneSymbolPipeline" still work.
// All real orchestration lives in multi_symbol_pipeline.hpp (Phase 4).
// =============================================================================

#include "multi_symbol_pipeline.hpp"

#include <stdexcept>
#include <vector>

namespace cog {

class OneSymbolPipeline {
public:
  explicit OneSymbolPipeline(SymbolId symbol, bool echo_stdout = true)
      : symbol_(symbol),
        // One shard only: symbol must be 0 for this convenience wrapper.
        // (MultiSymbolPipeline maps symbol_id → shard index directly.)
        multi_(1, echo_stdout) {
    if (symbol_ != 0) {
      // Routing uses symbol_id as shard index; a single-shard pipeline only
      // accepts symbol 0. Use MultiSymbolPipeline for other symbols.
      throw std::invalid_argument("OneSymbolPipeline only supports symbol_id 0; use MultiSymbolPipeline");
    }
  }

  EventLog& run(const std::vector<GatewayOrder>& orders) {
    // Force symbol 0 so callers cannot accidentally mix symbols into shard 0's
    // book (Phase 3 invariant).
    std::vector<GatewayOrder> normalized = orders;
    for (auto& go : normalized) {
      go.symbol_id = 0;
    }
    return multi_.run(normalized);
  }

  std::uint64_t ingress_drops() const { return multi_.ingress_drops(); }
  std::uint64_t event_drops() const { return multi_.event_drops(); }

  const lob::MatchingEngine& engine() const { return multi_.engine(0); }
  EventLog& log() { return multi_.log(); }
  const PipelineStats& stats() const { return multi_.stats(); }

private:
  SymbolId symbol_;
  MultiSymbolPipeline multi_;
};

}  // namespace cog
