cmake_minimum_required(VERSION 3.31)

project(hiredis_client)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

find_package(Threads REQUIRED)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${cflags}")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${cflags}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${cflags}")
set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} ${cflags}")

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${cflags}")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${cflags}")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} ${cflags}")
set(CMAKE_C_FLAGS_MINSIZEREL "${CMAKE_C_FLAGS_MINSIZEREL} ${cflags}")

include_directories(
    ${CMAKE_SOURCE_DIR}/include
    /opt/appier/xdep/include
    /opt/appier/cntk/include
)

link_directories(
    /opt/appier/xdep/lib
    /opt/appier/cntk/lib
)

add_executable(hiredis_client
    src/hiredis_client.cpp
)

target_link_libraries(hiredis_client
    PRIVATE
    cntk
    fmt
    hiredis
    yaml-cpp
    murmur3
    Threads::Threads
)

install(TARGETS hiredis_client RUNTIME DESTINATION bin)
