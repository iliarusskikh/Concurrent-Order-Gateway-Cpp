// =============================================================================
// test_one_symbol_e2e.cpp — Phase 3: gateway → queue → matcher → publisher.
//
// Asserts:
//   - Resting + crossing orders produce ACCEPT / TRADE lines via the pipeline
//   - Book state after run matches direct LOB expectations
//   - No ingress/event drops under light load
// =============================================================================

#include "gateway_order.hpp"
#include "one_symbol_pipeline.hpp"

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

bool contains_substr(const std::vector<std::string>& lines, const std::string& part) {
  for (const auto& l : lines) {
    if (l.find(part) != std::string::npos) return true;
  }
  return false;
}

void test_exact_fill_through_pipeline() {
  std::cout << "\n=== E2E exact fill (one symbol) ===\n";

  using lob::Side;
  using lob::OrderType;

  std::vector<cog::GatewayOrder> orders;
  orders.push_back(cog::make_gateway_order(
      0, {1, Side::Buy, OrderType::Limit, 100, 10}));
  orders.push_back(cog::make_gateway_order(
      0, {2, Side::Sell, OrderType::Limit, 100, 10}));

  // echo_stdout=false so test output stays readable
  cog::OneSymbolPipeline pipe(0, false);
  cog::EventLog& log = pipe.run(orders);
  const auto lines = log.snapshot();

  expect(pipe.ingress_drops() == 0, "no ingress drops");
  expect(pipe.event_drops() == 0, "no event drops");
  expect(contains_substr(lines, "ACCEPT id=1"), "accept buy 1");
  expect(contains_substr(lines, "ACCEPT id=2"), "accept sell 2");
  expect(contains_substr(lines, "TRADE maker=1 taker=2"), "trade maker=1 taker=2");
  expect(pipe.engine().bids(1).empty() && pipe.engine().asks(1).empty(),
         "book empty after exact fill");
}

void test_rest_then_partial() {
  std::cout << "\n=== E2E rest + partial fill ===\n";

  using lob::Side;
  using lob::OrderType;

  std::vector<cog::GatewayOrder> orders;
  orders.push_back(cog::make_gateway_order(
      0, {1, Side::Buy, OrderType::Limit, 100, 10}));
  orders.push_back(cog::make_gateway_order(
      0, {2, Side::Sell, OrderType::Limit, 100, 4}));

  cog::OneSymbolPipeline pipe(0, false);
  auto lines = pipe.run(orders).snapshot();

  expect(contains_substr(lines, "TRADE maker=1 taker=2 price=100 qty=4"),
         "partial trade qty=4");
  const auto bids = pipe.engine().bids(1);
  expect(!bids.empty() && bids[0].total_qty == 6, "residual bid qty=6");
}

}  // namespace

int main() {
  test_exact_fill_through_pipeline();
  test_rest_then_partial();

  std::cout << '\n';
  if (g_failures == 0) {
    std::cout << "All one_symbol_e2e tests passed.\n";
    return 0;
  }
  std::cerr << g_failures << " failure(s).\n";
  return 1;
}
