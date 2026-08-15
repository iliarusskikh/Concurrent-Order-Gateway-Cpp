// =============================================================================
// test_spsc_queue.cpp — Phase 2 unit test: producer / consumer transfer.
//
// WHAT THIS PROVES
//   1. try_push / try_pop move integers correctly on one thread (sanity).
//   2. Two threads (SPSC): producer pushes 1..N, consumer pops them in order.
//   3. ThreadGuard joins the consumer without std::terminate.
//
// Build:  cmake --build build --target gateway_tests
// Run:    ./build/gateway_tests
// =============================================================================

#include "gateway_order.hpp"
#include "spsc_queue.hpp"
#include "thread_guard.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
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

// --- Test A: single-threaded ring behaviour ---------------------------------
void test_try_push_pop_order() {
  std::cout << "\n=== SPSC try_push / try_pop (single thread) ===\n";
  cog::SpscQueue<int, 4> q;

  expect(q.empty(), "starts empty");
  expect(q.try_push(10), "push 10");
  expect(q.try_push(20), "push 20");
  expect(q.size() == 2, "size == 2");

  int x = 0;
  expect(q.try_pop(x) && x == 10, "pop 10 first (FIFO)");
  expect(q.try_pop(x) && x == 20, "pop 20 second");
  expect(!q.try_pop(x), "empty → try_pop false");
}

void test_full_try_push_fails() {
  std::cout << "\n=== SPSC full queue ===\n";
  cog::SpscQueue<int, 2> q;
  expect(q.try_push(1), "push 1");
  expect(q.try_push(2), "push 2");
  expect(!q.try_push(3), "full → try_push false (backpressure signal)");
  expect(q.full(), "reports full");
}

// --- Test B: two threads, ordered transfer ----------------------------------
void test_producer_consumer_ordered() {
  std::cout << "\n=== SPSC two-thread ordered transfer ===\n";

  constexpr int kCount = 1000;
  cog::SpscQueue<int, 64> q;
  std::atomic<bool> running{true};
  std::atomic<int> received_count{0};
  std::vector<int> received(static_cast<std::size_t>(kCount), -1);

  // Consumer thread: pop until we have kCount items.
  // ThreadGuard joins automatically when this scope ends / reset().
  cog::ThreadGuard consumer{std::thread([&] {
    for (int i = 0; i < kCount; ++i) {
      int v = 0;
      // Block until an item arrives. stop pred stays false until we finish.
      if (!q.pop_wait(v, [&] { return !running.load(); })) {
        break;  // shutdown with empty queue (should not happen in this test)
      }
      received[static_cast<std::size_t>(i)] = v;
      received_count.store(i + 1, std::memory_order_release);
    }
  })};

  // Producer (this thread): push 0 .. kCount-1
  for (int i = 0; i < kCount; ++i) {
    // Blocking push: this test checks ordering, not drop policy.
    int value = i;
    const bool ok = q.push_wait(std::move(value), [&] { return false; });
    expect(ok, "push_wait item " + std::to_string(i));
    if (!ok) break;
  }

  // Wait until consumer has stored every slot (atomic handshake — no data race).
  while (received_count.load(std::memory_order_acquire) < kCount) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  running = false;
  q.wake_all();
  consumer.join();

  expect(received_count.load() == kCount, "received all items");
  bool ordered = true;
  for (int i = 0; i < kCount; ++i) {
    if (received[static_cast<std::size_t>(i)] != i) {
      ordered = false;
      break;
    }
  }
  expect(ordered, "FIFO order preserved across threads");
}

// --- Test C: GatewayOrder moves through the queue ---------------------------
void test_gateway_order_message() {
  std::cout << "\n=== GatewayOrder through SPSC ===\n";
  cog::SpscQueue<cog::GatewayOrder, 8> q;

  auto go = cog::make_gateway_order(
      /*symbol=*/1,
      lob::OrderRequest{42, lob::Side::Buy, lob::OrderType::Limit, 100, 5});

  expect(q.try_push(std::move(go)), "enqueue GatewayOrder");

  cog::GatewayOrder out;
  expect(q.try_pop(out), "dequeue GatewayOrder");
  expect(out.symbol_id == 1, "symbol_id preserved");
  expect(out.request.order_id == 42, "order_id preserved");
  expect(out.request.qty == 5, "qty preserved");
}

}  // namespace

int main() {
  test_try_push_pop_order();
  test_full_try_push_fails();
  test_producer_consumer_ordered();
  test_gateway_order_message();

  std::cout << '\n';
  if (g_failures == 0) {
    std::cout << "All gateway_tests passed.\n";
    return 0;
  }
  std::cerr << g_failures << " failure(s).\n";
  return 1;
}
