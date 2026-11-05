file(GLOB srcs CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_executable(tdrctl ${srcs})
target_link_libraries(tdrctl PRIVATE tdr::ipc tdr::protocol tdr::common Boost::program_options Boost::json)
