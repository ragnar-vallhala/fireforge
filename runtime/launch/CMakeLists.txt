tdr_module(launch DEPS tdr::proto tdr::protocol tdr::common Boost::headers Boost::json)

add_executable(tdr-launch ${CMAKE_CURRENT_SOURCE_DIR}/app/main.cpp)
target_link_libraries(tdr-launch PRIVATE tdr::launch tdr::ipc tdr::protocol tdr::proto tdr::common)

if(TARGET tdr_launch_tests)
    add_dependencies(tdr_launch_tests tdr-launch)
    target_compile_definitions(tdr_launch_tests PRIVATE TDR_LAUNCH_BIN="$<TARGET_FILE:tdr-launch>")
endif()
