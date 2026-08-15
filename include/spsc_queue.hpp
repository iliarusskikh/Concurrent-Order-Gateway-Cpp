#pragma once

// =============================================================================
// spsc_queue.hpp — Bounded Single-Producer / Single-Consumer queue (v1).
//
// WHAT "SPSC" MEANS
//   Exactly ONE thread may call the push family (producer).
//   Exactly ONE other thread may call the pop family (consumer).
//   That invariant is part of the API contract — the type does not enforce it
//   at compile time; the architecture (one gateway, one worker per queue) does.
//
// WHY BOUNDED
//   Fixed capacity N. When full, try_push returns false (caller may drop+count).
//   Unbounded queues can grow forever under burst load and hide overload.
//
// WHY MUTEX + CONDITION VARIABLE (not lock-free yet)
//   Correct, readable, and easy to test. A lock-free ring can replace the
//   internals later behind the same method names if you want a follow-on.
//
// TYPICAL WIRING IN THIS PROJECT
//   Gateway thread  --try_push-->  SpscQueue<GatewayOrder, N>  --pop-->  matcher
// =============================================================================

#include <array>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <utility>

namespace cog {

// ---------------------------------------------------------------------------
// template<typename T, std::size_t N> class SpscQueue
//
//   T — element type (e.g. GatewayOrder). Moved in and out (no shared ownership).
//   N — capacity (number of slots). Must be >= 1.
//
//   Storage: std::array<T, N> used as a ring (circular buffer).
//   Indices: head_ = next slot to pop; tail_ = next slot to push; size_ = count.
// ---------------------------------------------------------------------------
template <typename T, std::size_t N>
class SpscQueue {
  static_assert(N >= 1, "SpscQueue capacity must be >= 1");

public:
  SpscQueue() = default;

  // Queues are shared between two threads by *reference* (or pointer).
  // Copying a queue would duplicate state incorrectly — forbid it.
  SpscQueue(const SpscQueue&) = delete;
  SpscQueue& operator=(const SpscQueue&) = delete;

  // -------------------------------------------------------------------------
  // capacity / size (approximate under concurrency — fine for metrics)
  // -------------------------------------------------------------------------

  // Compile-time / fixed slot count.
  static constexpr std::size_t capacity() noexcept { return N; }

  // How many items are currently stored. Takes the mutex briefly.
  std::size_t size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return size_;
  }

  bool empty() const {
    std::lock_guard<std::mutex> lock(mu_);
    return size_ == 0;
  }

  bool full() const {
    std::lock_guard<std::mutex> lock(mu_);
    return size_ == N;
  }

  // -------------------------------------------------------------------------
  // try_push — non-blocking produce
  //
  //   If there is room: move `item` into the ring, notify a waiting consumer,
  //   return true.
  //   If full: leave `item` alone (still in the caller's variable), return false.
  //
  //   v1 default backpressure policy at the gateway: if false → drop + atomic++.
  // -------------------------------------------------------------------------
  bool try_push(T&& item) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (size_ == N) {
        return false;
      }
      buf_[tail_] = std::move(item);
      tail_ = next(tail_);
      ++size_;
    }  // unlock before notify (good habit: don't wake someone who then waits on us)
    cv_not_empty_.notify_one();
    return true;
  }

  // Overload for lvalues: copies (or moves if T supports move-from const? no —
  // this copies into a temporary path via T copy). Prefer try_push(std::move(x)).
  bool try_push(const T& item) {
    T copy = item;
    return try_push(std::move(copy));
  }

  // -------------------------------------------------------------------------
  // push — blocking produce (optional demo / teaching mode)
  //
  //   Wait until there is room OR `stop` becomes true.
  //   Returns false if stopped while still full (shutdown path).
  //
  //   `stop` is typically an atomic<bool> checked via a callable so we don't
  //   hard-wire atomics into the queue type. Pass: [&]{ return !running.load(); }
  // -------------------------------------------------------------------------
  template <typename StopPred>
  bool push_wait(T&& item, StopPred&& stop) {
    std::unique_lock<std::mutex> lock(mu_);
    // Wait until not full, or caller wants to shut down.
    cv_not_full_.wait(lock, [&] {
      return size_ < N || stop();
    });
    if (size_ == N) {
      return false;  // stopped while full
    }
    buf_[tail_] = std::move(item);
    tail_ = next(tail_);
    ++size_;
    lock.unlock();
    cv_not_empty_.notify_one();
    return true;
  }

  // -------------------------------------------------------------------------
  // try_pop — non-blocking consume
  //
  //   If empty: return false (out unchanged).
  //   Else: move front element into `out`, notify a waiting producer, return true.
  // -------------------------------------------------------------------------
  bool try_pop(T& out) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (size_ == 0) {
        return false;
      }
      out = std::move(buf_[head_]);
      head_ = next(head_);
      --size_;
    }
    cv_not_full_.notify_one();
    return true;
  }

  // -------------------------------------------------------------------------
  // pop_wait — blocking consume
  //
  //   Wait until an item appears OR `stop` becomes true.
  //   Returns false on shutdown with empty queue (worker should exit loop).
  // -------------------------------------------------------------------------
  template <typename StopPred>
  bool pop_wait(T& out, StopPred&& stop) {
    std::unique_lock<std::mutex> lock(mu_);
    // Predicate loop: condition_variable can wake spuriously (without notify).
    // Always re-check the condition.
    cv_not_empty_.wait(lock, [&] {
      return size_ > 0 || stop();
    });
    if (size_ == 0) {
      return false;  // stopped and still empty
    }
    out = std::move(buf_[head_]);
    head_ = next(head_);
    --size_;
    lock.unlock();
    cv_not_full_.notify_one();
    return true;
  }

  // -------------------------------------------------------------------------
  // wake_all — call on shutdown so blocked push_wait / pop_wait can exit
  // -------------------------------------------------------------------------
  void wake_all() {
    cv_not_empty_.notify_all();
    cv_not_full_.notify_all();
  }

  // -------------------------------------------------------------------------
  // clear — drop all buffered items (single-threaded: call only when idle).
  //   Used between pipeline runs on the same object so a leftover item cannot
  //   leak into the next batch. Not for use while producer/consumer are live.
  // -------------------------------------------------------------------------
  void clear() {
    std::lock_guard<std::mutex> lock(mu_);
    head_ = 0;
    tail_ = 0;
    size_ = 0;
    // Elements left in buf_ are overwritten on the next push (moved-from / stale).
  }

private:
  // Advance ring index with wrap-around: 0,1,...,N-1,0,...
  static std::size_t next(std::size_t i) noexcept {
    return (i + 1) % N;
  }

  std::array<T, N> buf_{};   // the slots
  std::size_t head_{0};      // next pop index
  std::size_t tail_{0};      // next push index
  std::size_t size_{0};      // how many occupied

  // mutable so size()/empty()/full() can lock in const methods
  mutable std::mutex mu_;
  std::condition_variable cv_not_empty_;  // waiters: consumers
  std::condition_variable cv_not_full_;   // waiters: producers
};

}  // namespace cog
