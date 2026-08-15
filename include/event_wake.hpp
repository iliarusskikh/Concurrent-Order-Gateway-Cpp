#pragma once

// =============================================================================
// event_wake.hpp — Shared wake-up for the publisher when many shards push.
//
// WHY
//   Phase 4 keeps ONE event SPSC per matcher (still true SPSC: matcher i →
//   events[i] → publisher). The publisher must sleep until *any* shard has
//   work, or until all matchers have finished. This tiny helper is that bell.
// =============================================================================

#include <condition_variable>
#include <mutex>

namespace cog {

class EventWake {
public:
  // Matcher calls after try_push (and on exit) so the publisher can resume.
  void notify() {
    // Locking is not strictly required for notify_all, but pairing with wait
    // under the same mutex avoids subtle lost-wakeup races with the predicate.
    std::lock_guard<std::mutex> lock(mu_);
    cv_.notify_all();
  }

  // Publisher blocks until pred() is true (e.g. work available or shutdown).
  template <typename Pred>
  void wait(Pred pred) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, pred);
  }

private:
  std::mutex mu_;
  std::condition_variable cv_;
};

}  // namespace cog
