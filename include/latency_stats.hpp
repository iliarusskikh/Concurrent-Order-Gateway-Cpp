#pragma once

// =============================================================================
// latency_stats.hpp — Collect nanosecond samples; report p50 / p99 / min / max.
//
// WHAT WE MEASURE IN THIS PROJECT (local steady_clock — same process)
//   queue_wait       = match_started_at - enqueued_at   (ingress queueing)
//   match_duration   = matched_at - match_started_at    (MatchingEngine::submit)
//   match_to_publish = published_at - matched_at        (event queue + format)
//
// Honest about limits: these are NOT exchange wire RTTs. They are in-process
// pipeline stages — the right metric for a learning gateway.
// =============================================================================

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

namespace cog {

class LatencyStats {
public:
  // Thread-safe add (matchers may record in parallel into a shared collector).
  void add_ns(std::int64_t sample_ns) {
    if (sample_ns < 0) return;
    std::lock_guard<std::mutex> lock(mu_);
    samples_ns_.push_back(sample_ns);
  }

  std::size_t count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return samples_ns_.size();
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mu_);
    samples_ns_.clear();
  }

  // Human-readable one-liner, e.g.
  //   queue_wait: n=10 p50=1.2µs p99=4.0µs min=0.8µs max=5.1µs
  std::string report(const char* label) const {
    std::vector<std::int64_t> sorted;
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (samples_ns_.empty()) {
        return std::string(label) + ": no samples";
      }
      sorted = samples_ns_;
    }
    std::sort(sorted.begin(), sorted.end());

    std::ostringstream out;
    out << label << ": n=" << sorted.size()
        << " p50=" << format_us(percentile_ns(sorted, 0.50))
        << " p99=" << format_us(percentile_ns(sorted, 0.99))
        << " min=" << format_us(static_cast<double>(sorted.front()))
        << " max=" << format_us(static_cast<double>(sorted.back()));
    return out.str();
  }

private:
  static double percentile_ns(const std::vector<std::int64_t>& sorted, double p) {
    if (sorted.empty()) return 0.0;
    if (sorted.size() == 1) return static_cast<double>(sorted[0]);
    const double idx = p * static_cast<double>(sorted.size() - 1);
    const std::size_t lo = static_cast<std::size_t>(idx);
    const std::size_t hi = std::min(lo + 1, sorted.size() - 1);
    const double frac = idx - static_cast<double>(lo);
    return sorted[lo] * (1.0 - frac) + sorted[hi] * frac;
  }

  static std::string format_us(double ns) {
    // Print as microseconds with one decimal (readable for local queues).
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%.1fµs", ns / 1000.0);
    return buf;
  }

  mutable std::mutex mu_;
  std::vector<std::int64_t> samples_ns_;
};

// ---------------------------------------------------------------------------
// PipelineStats — the three histograms this gateway cares about.
// ---------------------------------------------------------------------------
struct PipelineStats {
  LatencyStats queue_wait;
  LatencyStats match_duration;
  LatencyStats match_to_publish;

  void clear() {
    queue_wait.clear();
    match_duration.clear();
    match_to_publish.clear();
  }

  std::string report_all() const {
    return queue_wait.report("queue_wait") + "\n" +
           match_duration.report("match_duration") + "\n" +
           match_to_publish.report("match_to_publish");
  }
};

}  // namespace cog
