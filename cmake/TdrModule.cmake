# tdr_module(<name> [DEPS <targets...>])
#
# Standard layout for a runtime library:
#   include/tdr/<name>/*.hpp   public headers
#   src/*.cpp                  implementation  -> target tdr_<name> (static)
#   tests/*.cpp                Boost.Test      -> target tdr_<name>_tests (ctest)
#
# Sources are globbed with CONFIGURE_DEPENDS, so adding a file needs no CMake edit.
function(tdr_module name)
    cmake_parse_arguments(ARG "" "" "DEPS" ${ARGN})
    set(target tdr_${name})

    file(GLOB_RECURSE srcs CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
    add_library(${target} STATIC ${srcs})
    add_library(tdr::${name} ALIAS ${target})
    target_include_directories(${target} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(${target} PUBLIC ${ARG_DEPS})

    file(GLOB_RECURSE tests CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
    if(tests)
        add_executable(${target}_tests ${tests} ${PROJECT_SOURCE_DIR}/runtime/testing/test_main.cpp)
        target_link_libraries(${target}_tests PRIVATE ${target} Boost::unit_test_framework)
        target_compile_definitions(${target}_tests PRIVATE BOOST_TEST_DYN_LINK)
        add_test(NAME ${target} COMMAND ${target}_tests)
        set_tests_properties(${target} PROPERTIES TIMEOUT 120)
    endif()
endfunction()
