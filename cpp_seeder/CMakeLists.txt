cmake_minimum_required(VERSION 3.20)
project(cdn_seed_project CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(python-bindings OFF CACHE BOOL "" FORCE)
set(build_tests OFF CACHE BOOL "" FORCE)
set(build_examples OFF CACHE BOOL "" FORCE)
set(build_tools OFF CACHE BOOL "" FORCE)
set(deprecated-functions OFF CACHE BOOL "" FORCE)

add_subdirectory(libtorrent-rasterbar-2.0.13)

find_package(OpenSSL REQUIRED)

add_executable(test_native_seed test_native_seed.cpp)
target_link_libraries(test_native_seed PRIVATE torrent-rasterbar)

set(CDN_FETCH_LIB ${CMAKE_CURRENT_SOURCE_DIR}/libcdn_fetch/target/release/liblibcdn_fetch.a)

add_custom_command(
    OUTPUT ${CDN_FETCH_LIB}
    COMMAND cargo build --release
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/libcdn_fetch
    COMMENT "Building Rust CDN fetch library (cargo build --release)"
)

add_custom_target(libcdn_fetch DEPENDS ${CDN_FETCH_LIB})

add_executable(cdn_seed cdn_seed.cpp cdn_disk_interface.cpp)
add_dependencies(cdn_seed libcdn_fetch)
target_link_libraries(cdn_seed PRIVATE
    torrent-rasterbar
    OpenSSL::SSL
    OpenSSL::Crypto
    ${CDN_FETCH_LIB}
    pthread
    dl
    m)

add_executable(test_leecher test_leecher.cpp)
target_link_libraries(test_leecher PRIVATE
    torrent-rasterbar
    OpenSSL::SSL
    OpenSSL::Crypto
    pthread
    dl
    m)
