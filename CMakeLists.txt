cmake_minimum_required(VERSION 3.25)
project(fireforge VERSION 0.1.0 LANGUAGES CXX)

# Build through the memory cap:   tools/build.sh   (see AGENTS.md)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

option(FF_SANITIZE "Build with AddressSanitizer + UBSan" OFF)
option(FF_WERROR "Treat warnings as errors" OFF)

add_compile_options(-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion)
if(FF_WERROR)
    add_compile_options(-Werror)
endif()
if(FF_SANITIZE)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

find_package(Threads REQUIRED)
find_package(Boost 1.83 CONFIG REQUIRED COMPONENTS unit_test_framework program_options json)
find_package(Protobuf REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(PostgreSQL REQUIRED)   # libpq (control plane)

enable_testing()
include(cmake/TdrModule.cmake)

add_definitions(-DTDR_VERSION="${PROJECT_VERSION}")

add_subdirectory(proto)

# Libraries, in dependency order.
add_subdirectory(runtime/common)
add_subdirectory(runtime/protocol)
add_subdirectory(runtime/ipc)
add_subdirectory(runtime/journal)
add_subdirectory(runtime/artifacts)
add_subdirectory(runtime/supervisor)
add_subdirectory(runtime/jobs)
add_subdirectory(runtime/tls)
add_subdirectory(runtime/http)
add_subdirectory(runtime/agent)
add_subdirectory(control/db)
add_subdirectory(control/api)

# Executables.
add_subdirectory(runtime/daemon)
add_subdirectory(runtime/ctl)
add_subdirectory(control/server)
