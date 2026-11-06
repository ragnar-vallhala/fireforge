# tdr_cpui: the web dashboard (docs/design/phase9.md §2). Every file under web/ except
# web/tests/ and dot-files is compiled into an asset table (embed.cmake).
tdr_module(cpui DEPS tdr::http tdr::common OpenSSL::Crypto)

set(web ${CMAKE_CURRENT_SOURCE_DIR}/web)
# CONFIGURE_DEPENDS: adding or removing a file re-runs the glob at build time.
file(GLOB_RECURSE web_files CONFIGURE_DEPENDS LIST_DIRECTORIES false ${web}/*)
list(FILTER web_files EXCLUDE REGEX "^${web}/tests/")
list(FILTER web_files EXCLUDE REGEX "/\\.[^/]*$")
list(SORT web_files)
# The list's hash is part of the command, so removing a file also regenerates.
string(MD5 web_list_hash "${web_files}")

set(gen ${CMAKE_CURRENT_BINARY_DIR}/ui_assets.cpp)
add_custom_command(
    OUTPUT ${gen}
    COMMAND ${CMAKE_COMMAND} -DWEB=${web} -DOUT=${gen} -DLIST_HASH=${web_list_hash}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/embed.cmake
    DEPENDS ${web_files} ${CMAKE_CURRENT_SOURCE_DIR}/embed.cmake
    COMMENT "Embedding control/ui/web (${CMAKE_CURRENT_SOURCE_DIR})"
    VERBATIM)
target_sources(tdr_cpui PRIVATE ${gen})
target_include_directories(tdr_cpui PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
if(TARGET tdr_cpui_tests)
    target_compile_definitions(tdr_cpui_tests PRIVATE TDR_UI_WEB_DIR="${web}")
endif()
