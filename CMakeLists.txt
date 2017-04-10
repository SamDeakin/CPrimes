cmake_minimum_required(VERSION 3.6)
project(CPrimes)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        main.cc)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib/tbb/include)

set(TBB_PATH_HINT ${CMAKE_CURRENT_SOURCE_DIR}/lib/tbb/build/)
find_file(TBB_LIB NAMES libtbb.dylib HINTS ${TBB_PATH_HINT} NO_CMAKE_FIND_ROOT_PATH)

add_executable(CPrimes ${SOURCE_FILES})
target_link_libraries(CPrimes ${TBB_LIB})
