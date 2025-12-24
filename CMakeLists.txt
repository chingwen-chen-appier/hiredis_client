cmake_minimum_required(VERSION 3.16)

project(RedisBenchmark VERSION 1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories
include_directories(
    /opt/appier/xdep/include
    /opt/appier/cntk/include
)

# Library directories
link_directories(
    /opt/appier/xdep/lib
    /opt/appier/cntk/lib
)

# Add executable
add_executable(bench
    bench.cpp
)

# Link libraries
target_link_libraries(bench
    cntk
    cntk_proc
    cntk_service
    fmt
    hiredis
    yaml-cpp
    murmur3
    pthread
)
