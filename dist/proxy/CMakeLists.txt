cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project(proxy)

#set(Torch_DIR /home/xian/libtorch17/libtorch/share/cmake/Torch)
#find_package(Torch REQUIRED)
#find_package(OpenCV REQUIRED)
#include_directories("${OpenCV_INCLUDE_DIRS}")

add_executable(proxy proxy.cpp)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

#target_link_libraries(proxy /usr/local/lib/libwiredtiger.so)
target_link_libraries(proxy PRIVATE Threads::Threads)
#target_link_libraries(server "${TORCH_LIBRARIES}")
#target_link_libraries(server ${OpenCV_LIBS})

set_property(TARGET proxy PROPERTY CXX_STANDARD 14)
