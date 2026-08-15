// =============================================================================
// test_multi_symbol.cpp — Phase 4: parallel shards, isolated books.
// =============================================================================

#include "gateway_order.hpp"
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

bool has_line(const std::vector<std::string>& lines, const std::string& part) {
  for (const auto& l : lines) {
    if (l.find(part) != std::string::npos) return true;
  }
  return false;
}

void test_two_symbols_parallel_fills() {
  std::cout << "\n=== Multi-symbol parallel fills ===\n";
  using lob::Side;
  using lob::OrderType;

  std::vector<cog::GatewayOrder> orders;
  // Symbol 0: exact fill
  orders.push_back(cog::make_gateway_order(
      0, {1, Side::Buy, OrderType::Limit, 100, 10}));
  orders.push_back(cog::make_gateway_order(
      0, {2, Side::Sell, OrderType::Limit, 100, 10}));
  // Symbol 1: exact fill (same order ids OK — different engines)
  orders.push_back(cog::make_gateway_order(
      1, {1, Side::Buy, OrderType::Limit, 50, 5}));
  orders.push_back(cog::make_gateway_order(
      1, {2, Side::Sell, OrderType::Limit, 50, 5}));

  cog::MultiSymbolPipeline pipe(/*num_symbols=*/4, /*echo=*/false);
  const auto lines = pipe.run(orders).snapshot();

  expect(pipe.ingress_drops() == 0, "no ingress drops");
  expect(pipe.event_drops() == 0, "no event drops");
  expect(pipe.unknown_symbol_drops() == 0, "no unknown-symbol drops");
  expect(has_line(lines, "sym=0 TRADE maker=1 taker=2"), "sym0 trade");
  expect(has_line(lines, "sym=1 TRADE maker=1 taker=2"), "sym1 trade");
  expect(pipe.engine(0).bids(1).empty() && pipe.engine(0).asks(1).empty(),
         "sym0 book empty");
  expect(pipe.engine(1).bids(1).empty() && pipe.engine(1).asks(1).empty(),
         "sym1 book empty");
  // Unused shards stay empty / unused — engines 2,3 never received orders.
  expect(pipe.engine(2).bids(1).empty(), "sym2 untouched");
}

void test_unknown_symbol_dropped() {
  std::cout << "\n=== Unknown symbol drop ===\n";
  using lob::Side;
  using lob::OrderType;

  std::vector<cog::GatewayOrder> orders;
  orders.push_back(cog::make_gateway_order(
      9, {1, Side::Buy, OrderType::Limit, 100, 1}));  // only 2 shards

  cog::MultiSymbolPipeline pipe(2, false);
  pipe.run(orders);
  expect(pipe.unknown_symbol_drops() == 1, "symbol 9 counted as unknown");
  expect(pipe.log().size() == 0, "no events published for unknown symbol");
}

void test_books_isolated() {
  std::cout << "\n=== Books isolated across symbols ===\n";
  using lob::Side;
  using lob::OrderType;

  // Rest buy on sym0 only — must not interact with sym1 sell.
  std::vector<cog::GatewayOrder> orders;
  orders.push_back(cog::make_gateway_order(
      0, {1, Side::Buy, OrderType::Limit, 100, 10}));
  orders.push_back(cog::make_gateway_order(
      1, {2, Side::Sell, OrderType::Limit, 100, 10}));

  cog::MultiSymbolPipeline pipe(2, false);
  const auto lines = pipe.run(orders).snapshot();

  expect(!has_line(lines, "TRADE"), "no cross-symbol trade");
  expect(!pipe.engine(0).bids(1).empty(), "sym0 still has bid");
  expect(!pipe.engine(1).asks(1).empty(), "sym1 still has ask");
}

}  // namespace

int main() {
  test_two_symbols_parallel_fills();
  test_unknown_symbol_dropped();
  test_books_isolated();

  std::cout << '\n';
  if (g_failures == 0) {
    std::cout << "All multi_symbol tests passed.\n";
    return 0;
  }
  std::cerr << g_failures << " failure(s).\n";
  return 1;
}
