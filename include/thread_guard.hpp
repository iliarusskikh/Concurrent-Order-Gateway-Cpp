#pragma once

// =============================================================================
// thread_guard.hpp — RAII wrapper that joins a std::thread on destruction.
//
// WHY THIS EXISTS
//   std::thread is joinable after you start it. If you destroy a joinable
//   thread without join() or detach(), C++ calls std::terminate() — hard crash.
//
//   ThreadGuard ties "this thread must be joined" to an object's lifetime:
//     construct → you own a running thread
//     destroy   → join() so the worker finishes cleanly
//
//   Same idea as SocketGuard in the TCP MDS project, but for threads not fds.
//
// HOW WE USE IT (later phases)
//   Main creates ThreadGuard for gateway / matcher / publisher workers.
//   When main returns (or an early error unwinds the stack), destructors join
//   automatically — you do not need a manual join checklist on every path.
// =============================================================================

#include <thread>
#include <utility>

namespace cog {

// ---------------------------------------------------------------------------
// class ThreadGuard
//
//   Move-only owner of one std::thread.
//   - Copy deleted: two guards must not own the same thread.
//   - Move steals the thread; moved-from guard is empty (destructor no-ops).
// ---------------------------------------------------------------------------
class ThreadGuard {
public:
  // Default: empty guard, no thread. Useful as a placeholder member.
  ThreadGuard() noexcept = default;

  // Take ownership of an already-constructed std::thread (usually from
  // std::thread{lambda} temporary via ThreadGuard{std::thread{...}}).
  explicit ThreadGuard(std::thread&& t) noexcept : t_(std::move(t)) {}

  // Join if still joinable. Safe to call multiple times conceptually via reset.
  ~ThreadGuard() { reset(); }

  // Non-copyable (Rule of Five: if you define dtor/move, control copy too).
  ThreadGuard(const ThreadGuard&) = delete;
  ThreadGuard& operator=(const ThreadGuard&) = delete;

  // Move constructor: steal other's thread; leave other empty.
  ThreadGuard(ThreadGuard&& other) noexcept : t_(std::move(other.t_)) {}

  // Move assignment: join any thread we currently own, then steal other's.
  ThreadGuard& operator=(ThreadGuard&& other) noexcept {
    if (this != &other) {
      reset();
      t_ = std::move(other.t_);
    }
    return *this;
  }

  // True if we hold a thread that still needs join/detach.
  bool joinable() const noexcept { return t_.joinable(); }

  // Access the underlying thread (e.g. rare advanced use). Prefer join via dtor.
  std::thread& get() noexcept { return t_; }
  const std::thread& get() const noexcept { return t_; }

  // Explicitly join now (also called from destructor).
  // After join, joinable() is false.
  void join() {
    if (t_.joinable()) {
      t_.join();
    }
  }

  // Join current thread (if any) and become empty.
  void reset() noexcept {
    if (t_.joinable()) {
      // join() can theoretically throw (system_error); for a learning project
      // we still join in the destructor path — prefer clean shutdown design
      // so workers always exit when "running" becomes false.
      t_.join();
    }
  }

private:
  std::thread t_{};  // default-constructed thread is not joinable
};

}  // namespace cog
