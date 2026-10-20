cmake_minimum_required(VERSION 3.25)
project(relaymq-cpp
    VERSION 1.0.0
    DESCRIPTION "C++ client library for RelayMQ"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wshadow
    -Wnon-virtual-dtor
)

find_package(Threads REQUIRED)

add_library(relaymq_client
    src/protocol.cpp
    src/client.cpp
)

add_library(relaymq::client ALIAS relaymq_client)

target_include_directories(relaymq_client PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(relaymq_client PUBLIC
    Threads::Threads
)

if(PROJECT_IS_TOP_LEVEL)
    enable_testing()
    include(FetchContent)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
    )

    set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)

    FetchContent_MakeAvailable(googletest)

    add_executable(relaymq_cpp_tests
        tests/test_endpoint.cpp
        tests/test_protocol.cpp
        tests/test_client.cpp
    )
    target_link_libraries(relaymq_cpp_tests PRIVATE
        relaymq_client
        GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(relaymq_cpp_tests)

    add_executable(relay-bench
        benchmarks/bench_main.cpp
    )
    target_link_libraries(relay-bench PRIVATE
        relaymq_client
    )
endif()
