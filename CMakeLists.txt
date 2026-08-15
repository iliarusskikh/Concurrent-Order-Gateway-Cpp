cmake_minimum_required(VERSION 3.16)
# Concurrent Order Gateway — C++17, zero third-party deps.
#
# Targets:
#   lob_tests       — vendored LOB golden tests (G1–G9)
#   gateway_tests   — SPSC + ThreadGuard unit tests
#   e2e_tests       — one-symbol pipeline end-to-end
#   multi_tests     — multi-symbol sharding tests
#   gateway         — CLI scenario runner
project(ConcurrentOrderGateway LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(cog_core INTERFACE)
target_include_directories(cog_core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_options(cog_core INTERFACE -Wall -Wextra -Wpedantic)

find_package(Threads REQUIRED)

add_executable(lob_tests tests/test_lob_golden.cpp)
target_link_libraries(lob_tests PRIVATE cog_core)

add_executable(gateway_tests tests/test_spsc_queue.cpp)
target_link_libraries(gateway_tests PRIVATE cog_core Threads::Threads)

add_executable(e2e_tests tests/test_one_symbol_e2e.cpp)
target_link_libraries(e2e_tests PRIVATE cog_core Threads::Threads)

add_executable(multi_tests tests/test_multi_symbol.cpp)
target_link_libraries(multi_tests PRIVATE cog_core Threads::Threads)

add_executable(latency_tests tests/test_latency.cpp)
target_link_libraries(latency_tests PRIVATE cog_core Threads::Threads)

add_executable(gateway src/main.cpp)
target_link_libraries(gateway PRIVATE cog_core Threads::Threads)

enable_testing()
add_test(NAME lob_tests COMMAND lob_tests)
add_test(NAME gateway_tests COMMAND gateway_tests)
add_test(NAME e2e_tests COMMAND e2e_tests)
add_test(NAME multi_tests COMMAND multi_tests)
add_test(NAME latency_tests COMMAND latency_tests)
