# Generated C++ protobuf code -> target tdr_proto (tdr::proto).
# Include as:  #include "fireforge/tdr/v1/job.pb.h"
file(GLOB protos CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/fireforge/tdr/v1/*.proto)
add_library(tdr_proto STATIC ${protos})
add_library(tdr::proto ALIAS tdr_proto)
protobuf_generate(
    TARGET tdr_proto
    LANGUAGE cpp
    IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
    PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(tdr_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(tdr_proto PUBLIC protobuf::libprotobuf)
# Generated code is not ours; silence our strict warnings for it.
target_compile_options(tdr_proto PRIVATE -w)
