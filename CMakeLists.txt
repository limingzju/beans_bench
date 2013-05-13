cmake_minimum_required(VERSION 2.8)
#add_definitions(" -Wall -Wno-sign-compare -Werror -O2 ")
#add_definitions(" -Wall -Wno-sign-compare -O2 ")
#add_definitions("-Wall -g -fpermissive")
add_definitions("-Wall -O2 -g -D_FILE_OFFSET_BITS=64 -D__STDC_FORMAT_MACROS ")

set(THIRD_PARTY_DIR "/home/liming/Projects")
set(GPROFILER_DIR "${THIRD_PARTY_DIR}/gperftools")
include_directories(
    "${GPROFILER_DIR}/include";
    )
link_directories(
    "${GPROFILER_DIR}/lib";
    )
#set(CMAKE_C_COMPILER "g++")
add_library(beansdb bitcask.c codec.c diskmgr.c hstore.c record.c clock_gettime_stub.c crc32.c hint.c htree.c quicklz.c fnv1a.c)

add_executable(db_bench db_bench.cc histogram.cc concurrent.cc string_util.cc)
target_link_libraries(db_bench beansdb libprofiler.a pthread)


