cmake_minimum_required(VERSION 3.22)
project(project)

option(ENABLE_AVX2_ASM "Enable AVX2 NASM acceleration (may SIGILL on older CPUs)" OFF)

set(ASM_SOURCES "")
if(ENABLE_AVX2_ASM)
  # Enable assembly language support (NASM)
  enable_language(ASM_NASM)

  set(CMAKE_ASM_NASM_SOURCE_FILE_EXTENSIONS asm)  # recognize .asm files
  set(CMAKE_ASM_NASM_COMPILER "/usr/bin/nasm")    # specify NASM
  set(CMAKE_ASM_NASM_OBJECT_FORMAT elf64)
  set(CMAKE_ASM_NASM_COMPILE_OBJECT "<CMAKE_ASM_NASM_COMPILER> <INCLUDES> <FLAGS> -f elf64 -o <OBJECT> <SOURCE>")
  set(CMAKE_ASM_NASM_OUTPUT_EXTENSION .o)
  # Include path for NASM
  set(CMAKE_ASM_NASM_FLAGS "-f elf64 -I${CMAKE_CURRENT_SOURCE_DIR}/src/asm")

  set(ASM_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/src/asm/reg_sizes.asm
      ${CMAKE_CURRENT_SOURCE_DIR}/src/asm/gf_vect_dot_prod_avx2.asm
      ${CMAKE_CURRENT_SOURCE_DIR}/src/asm/gf_2vect_dot_prod_avx2.asm
      ${CMAKE_CURRENT_SOURCE_DIR}/src/asm/gf_3vect_dot_prod_avx2.asm
      ${CMAKE_CURRENT_SOURCE_DIR}/src/asm/gf_4vect_dot_prod_avx2.asm
      ${CMAKE_CURRENT_SOURCE_DIR}/src/asm/gf_5vect_dot_prod_avx2.asm
      ${CMAKE_CURRENT_SOURCE_DIR}/src/asm/gf_6vect_dot_prod_avx2.asm
      ${CMAKE_CURRENT_SOURCE_DIR}/src/asm/xor_gen_avx.asm
  )

  set_source_files_properties(
      ${ASM_SOURCES}
      PROPERTIES
      LANGUAGE ASM_NASM
  )

  foreach(_t run_coordinator run_proxy run_datanode main_client)
    # Only defined for compilation units (not protobuf-generated)
    add_compile_definitions(ENABLE_AVX2_ASM)
  endforeach()
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-gdwarf-4 -gstrict-dwarf -g -Wall -std=c++17 -DASIO_STANDALONE -O3 -funroll-loops -fdata-sections -ffunction-sections")

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    option(ENABLE_MARCH_NATIVE "Enable -march=native (may break portability)" OFF)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -funroll-loops -ftree-vectorize")
    if(ENABLE_MARCH_NATIVE)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -unroll -qopt-report -xHost")
endif()

find_package(Threads REQUIRED)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/include")
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/third_party/grpc")
set(spdlog_DIR ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
if(CMAKE_CROSSCOMPILING)
  find_program(_PROTOBUF_PROTOC protoc)
else()
  set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
message(STATUS "gRPC_DIR = ${gRPC_DIR}")
message(STATUS "gRPC_INCLUDE_DIRS = ${gRPC_INCLUDE_DIRS}")
message(STATUS "gRPC_LIBS = ${gRPC_LIBS}")

set(_GRPC_GRPCPP gRPC::grpc++)
if(CMAKE_CROSSCOMPILING)
  find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
  set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

# Proto file
get_filename_component(cn_proto "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/coordinator.proto" ABSOLUTE)
get_filename_component(pn_proto "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/proxy.proto" ABSOLUTE)
get_filename_component(dn_proto "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/datanode.proto" ABSOLUTE)
get_filename_component(cn_proto_path "${cn_proto}" PATH)
message("${cn_proto}")
message("${cn_proto_path}")

set(cn_proto_srcs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/coordinator.pb.cc")
set(cn_proto_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/coordinator.pb.h")
set(cn_grpc_srcs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/coordinator.grpc.pb.cc")
set(cn_grpc_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/coordinator.grpc.pb.h")
set(pn_proto_srcs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/proxy.pb.cc")
set(pn_proto_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/proxy.pb.h")
set(pn_grpc_srcs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/proxy.grpc.pb.cc")
set(pn_grpc_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/proxy.grpc.pb.h")
set(dn_proto_srcs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/datanode.pb.cc")
set(dn_proto_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/datanode.pb.h")
set(dn_grpc_srcs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/datanode.grpc.pb.cc")
set(dn_grpc_hdrs "${CMAKE_CURRENT_SOURCE_DIR}/src/proto/datanode.grpc.pb.h")

add_custom_command(
        OUTPUT ${cn_proto_srcs} ${cn_proto_hdrs} ${cn_grpc_srcs} ${cn_grpc_hdrs} ${pn_proto_srcs} ${pn_proto_hdrs} ${pn_grpc_srcs} ${pn_grpc_hdrs} ${dn_proto_srcs} ${dn_proto_hdrs} ${dn_grpc_srcs} ${dn_grpc_hdrs}
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_SOURCE_DIR}/src/proto"
        --cpp_out "${CMAKE_CURRENT_SOURCE_DIR}/src/proto"
        -I "${CMAKE_CURRENT_SOURCE_DIR}/src/proto"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${cn_proto}" "${pn_proto}" "${dn_proto}"
        DEPENDS "${cn_proto}" "${pn_proto}" "${dn_proto}")

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src/proto")
# include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/libmemcached/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/asio/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/gf-complete/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/jerasure/include/jerasure)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/jerasure/include)

# link_directories("${CMAKE_CURRENT_SOURCE_DIR}/third_party/libmemcached/lib")
link_directories("${CMAKE_CURRENT_SOURCE_DIR}/third_party/jerasure/lib")
link_directories("${CMAKE_CURRENT_SOURCE_DIR}/third_party/gf-complete/lib")
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib)

set(Boost_USE_STATIC_LIBS ON)
#find_package(Boost COMPONENTS thread REQUIRED)

# for gdb debug
# add_compile_options(-gdwarf-4 -gstrict-dwarf)

# set(testsrc ${CMAKE_CURRENT_SOURCE_DIR}/test/test.cpp)
# add_executable(unittest ${testsrc})
# target_link_libraries(unittest libJerasure.a libgf_complete.a)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/   PROJECT_SRC)

# Targets (client|server)
foreach (_target
       run_coordinator run_proxy run_datanode main_client)
    add_executable(${_target}
            ${cn_proto_srcs}
            ${cn_grpc_srcs}
            ${pn_proto_srcs}
            ${pn_grpc_srcs}
            ${dn_proto_srcs}
            ${dn_grpc_srcs}
            ${ASM_SOURCES}
            ${CMAKE_CURRENT_SOURCE_DIR}/src/client.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/src/coordinator.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/src/proxy.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/run_cpp/${_target}.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/src/toolbox.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/src/tinyxml2.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/src/datanode.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/src/lrc.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/src/config.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/src/unilrc_encoder.cpp
            #${_target}.cpp
            )
    target_link_libraries(${_target}
            ${_REFLECTION}
            ${_GRPC_GRPCPP}
	    ${_GRPC_GRPCPP_UNSECURE}
            ${_PROTOBUF_LIBPROTOBUF}
            # -lmemcached
            -lpthread
            -lJerasure -lgf_complete
            )
endforeach ()