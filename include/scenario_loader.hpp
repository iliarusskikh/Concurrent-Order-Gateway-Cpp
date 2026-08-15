#pragma once

// =============================================================================
// scenario_loader.hpp — Parse a tiny DSL file into GatewayOrder list (Phase 3).
//
// SUPPORTED LINES (one symbol implied, or explicit):
//   NEW <id> <BUY|SELL> <LIMIT|MARKET> <qty> <price>
//   NEW <id> <BUY|SELL> <LIMIT|MARKET> <qty> <price> SYM <symbol_id>
//   CANCEL <id>                  (Phase 3.1 — parsed but optional)
//   CANCEL <id> SYM <symbol_id>
//   # comment / blank lines ignored
//
// WHY A TINY PARSER HERE
//   Full LOB scenario.hpp also has CHECK_* commands. Gateway Phase 3 only needs
//   to *feed* orders into the pipeline; assertions live in C++ tests.
// =============================================================================

#include "gateway_order.hpp"
#include "lob/types.hpp"

#include <cctype>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace cog {

namespace detail {

inline std::string upper(std::string s) {
  for (char& c : s) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return s;
}

inline lob::Side parse_side(const std::string& s) {
  const std::string u = upper(s);
  if (u == "BUY") return lob::Side::Buy;
  if (u == "SELL") return lob::Side::Sell;
  throw std::runtime_error("bad side: " + s);
}

inline lob::OrderType parse_type(const std::string& s) {
  const std::string u = upper(s);
  if (u == "LIMIT") return lob::OrderType::Limit;
  if (u == "MARKET") return lob::OrderType::Market;
  throw std::runtime_error("bad type: " + s);
}

}  // namespace detail

// ---------------------------------------------------------------------------
// load_scenario_file
//   Returns GatewayOrders in file order. default_symbol used when SYM omitted.
// ---------------------------------------------------------------------------
inline std::vector<GatewayOrder> load_scenario_file(const std::string& path, SymbolId default_symbol = 0) {
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("cannot open scenario: " + path);
  }

  std::vector<GatewayOrder> out;
  std::string line;
  std::size_t line_no = 0;

  while (std::getline(in, line)) {
    ++line_no;
    // Strip comments
    const auto hash = line.find('#');
    if (hash != std::string::npos) {
      line.resize(hash);
    }
    std::istringstream iss(line);
    std::string cmd;
    // check if the line is empty or only contains whitespace 
    //  reads one whitespace-separated token.
    if (!(iss >> cmd)) {
      continue;  // blank
    }
    cmd = detail::upper(cmd);

    if (cmd == "NEW") {
      std::uint64_t id = 0;
      std::string side_s, type_s;
      std::int64_t qty = 0, price = 0;
      if (!(iss >> id >> side_s >> type_s >> qty >> price)) {
        throw std::runtime_error("NEW parse error at line " +
                                 std::to_string(line_no));
      }
      SymbolId sym = default_symbol;
      std::string maybe_sym;
      if (iss >> maybe_sym) {
        if (detail::upper(maybe_sym) != "SYM") {
          throw std::runtime_error("expected SYM at line " +
                                   std::to_string(line_no));
        }
        unsigned long sid = 0;
        if (!(iss >> sid)) {
          throw std::runtime_error("bad SYM id at line " +
                                   std::to_string(line_no));
        }
        sym = static_cast<SymbolId>(sid);
      }

      lob::OrderRequest req{id, detail::parse_side(side_s), detail::parse_type(type_s), price,qty};
      // Stamp time at load for simplicity; gateway may re-stamp on enqueue.
      out.push_back(make_gateway_order(sym, req));
    } else if (cmd == "CANCEL") {
      // Phase 3 matcher currently handles submit only. Keep parse support so
      // scenarios can grow; skip CANCEL in the loader output for now by
      // encoding as order_id with a sentinel? Simpler: ignore CANCEL lines
      // with a note — or store as special. For Phase 3 demos we only use NEW.
      // Skipping keeps the pipeline focused.
      continue;
    } else {
      throw std::runtime_error("unknown command '" + cmd + "' at line " +
                               std::to_string(line_no));
    }
  }
  return out;
}

}  // namespace cog
