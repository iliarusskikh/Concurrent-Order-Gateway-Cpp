#pragma once

// =============================================================================
// event_log.hpp — Thread-safe sink for formatted event lines.
//
// WHY
//   Publisher runs on its own thread. Tests (and optionally main) need to read
//   what was published after shutdown. A mutex-protected vector is enough for v1.
// =============================================================================

#include <mutex>
#include <string>
#include <vector>

namespace cog {

class EventLog {
public:
  // Append one line (called from the publisher thread).
  void append(std::string line) {
    std::lock_guard<std::mutex> lock(mu_);
    lines_.push_back(std::move(line));
  }

  // Snapshot for tests / reporting (call after publisher has joined).
  std::vector<std::string> snapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    return lines_;
  }

  std::size_t size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return lines_.size();
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mu_);
    lines_.clear();
  }

private:
  mutable std::mutex mu_;
  std::vector<std::string> lines_;
};

}  // namespace cog
