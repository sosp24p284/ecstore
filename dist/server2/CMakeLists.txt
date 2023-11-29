cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project(server)

#set(Torch_DIR /home/xian/libtorch17/libtorch/share/cmake/Torch)
#find_package(Torch REQUIRED)
#find_package(OpenCV REQUIRED)
#include_directories("${OpenCV_INCLUDE_DIRS}")

add_executable(server server.cpp)
#target_link_libraries(server "${TORCH_LIBRARIES}")
#target_link_libraries(server ${OpenCV_LIBS})
target_link_libraries(server /usr/local/lib/libwiredtiger.so)

set_property(TARGET server PROPERTY CXX_STANDARD 14)
