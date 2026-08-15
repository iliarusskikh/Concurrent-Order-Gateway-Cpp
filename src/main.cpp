// =============================================================================
// src/main.cpp — CLI: scenario → multi-symbol pipeline + latency report.
//
// Usage:
//   ./gateway                                 # multi_symbol_demo.txt, 4 shards
//   ./gateway path/to/scenario.txt [num_symbols]
// =============================================================================

#include "multi_symbol_pipeline.hpp"
#include "scenario_loader.hpp"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char** argv) {
  const std::string path = (argc >= 2) ? argv[1] : std::string("scenarios/multi_symbol_demo.txt");
  const std::size_t num_symbols = (argc >= 3) ? static_cast<std::size_t>(std::strtoul(argv[2], nullptr, 10)) : 4;

  try {
    std::vector<cog::GatewayOrder> orders = cog::load_scenario_file(path, /*default_symbol=*/0);

    std::cout << "Concurrent Order Gateway — multi-symbol pipeline\n" << "scenario: " << path << " (" << orders.size() << " orders)\n"
              << "shards: " << num_symbols << "\n" << "---- events ----\n";

    cog::MultiSymbolPipeline pipe(num_symbols, /*echo_stdout=*/true);
    cog::EventLog& log = pipe.run(orders);

    std::cout << "---- done ----\n" << "published_lines=" << log.size()
              << " ingress_drops=" << pipe.ingress_drops()
              << " event_drops=" << pipe.event_drops()
              << " unknown_symbol_drops=" << pipe.unknown_symbol_drops()
              << "\n---- latency (steady_clock, in-process) ----\n"
              << pipe.stats().report_all() << '\n';
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << '\n';
    return 1;
  }
  return 0;
}
