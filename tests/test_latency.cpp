// =============================================================================
// test_latency.cpp — Phase 5: pipeline records the three latency histograms.
// =============================================================================

#include "gateway_order.hpp"
#include "latency_stats.hpp"
#include "multi_symbol_pipeline.hpp"

#include <iostream>
#include <string>
#include <vector>

namespace {

int g_failures = 0;

void expect(bool cond, const std::string& msg) {
  if (!cond) {
    std::cerr << "FAIL: " << msg << '\n';
    ++g_failures;
  } else {
    std::cout << "ok: " << msg << '\n';
  }
}

void test_stats_unit() {
  std::cout << "\n=== LatencyStats unit ===\n";
  cog::LatencyStats s;
  s.add_ns(1000);
  s.add_ns(2000);
  s.add_ns(3000);
  expect(s.count() == 3, "count == 3");
  const std::string r = s.report("demo");
  expect(r.find("p50=") != std::string::npos, "report has p50");
  expect(r.find("n=3") != std::string::npos, "report has n=3");
}

void test_pipeline_records_histograms() {
  std::cout << "\n=== Pipeline histograms ===\n";
  using lob::Side;
  using lob::OrderType;

  std::vector<cog::GatewayOrder> orders;
  for (int i = 0; i < 20; ++i) {
    orders.push_back(cog::make_gateway_order(
        static_cast<cog::SymbolId>(i % 2),
        {static_cast<std::uint64_t>(i + 1), Side::Buy, OrderType::Limit,
         100 + (i % 3), 1}));
  }
  // Cross some liquidity on both symbols
  orders.push_back(cog::make_gateway_order(
      0, {1000, Side::Sell, OrderType::Limit, 100, 5}));
  orders.push_back(cog::make_gateway_order(
      1, {1001, Side::Sell, OrderType::Limit, 100, 5}));

  cog::MultiSymbolPipeline pipe(2, false);
  pipe.run(orders);

  expect(pipe.stats().queue_wait.count() >= 20, "queue_wait samples per order");
  expect(pipe.stats().match_duration.count() >= 20,
         "match_duration samples per order");
  expect(pipe.stats().match_to_publish.count() > 0,
         "match_to_publish samples per published event");

  const std::string report = pipe.stats().report_all();
  expect(report.find("queue_wait:") != std::string::npos, "report queue_wait");
  expect(report.find("match_duration:") != std::string::npos,
         "report match_duration");
  expect(report.find("match_to_publish:") != std::string::npos,
         "report match_to_publish");
}

}  // namespace

int main() {
  test_stats_unit();
  test_pipeline_records_histograms();

  std::cout << '\n';
  if (g_failures == 0) {
    std::cout << "All latency tests passed.\n";
    return 0;
  }
  std::cerr << g_failures << " failure(s).\n";
  return 1;
}
