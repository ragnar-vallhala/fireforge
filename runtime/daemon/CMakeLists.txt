file(GLOB srcs CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_executable(tdrd ${srcs})
target_link_libraries(tdrd PRIVATE tdr::remote tdr::peer tdr::agent tdr::http tdr::tls tdr::jobs tdr::ipc tdr::journal tdr::artifacts tdr::supervisor
    tdr::protocol tdr::common Boost::program_options)
