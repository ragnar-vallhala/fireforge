file(GLOB srcs CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_executable(tdrcp ${srcs})
target_link_libraries(tdrcp PRIVATE tdr::cpapi tdr::cpdb tdr::http tdr::tls tdr::common Boost::program_options)
