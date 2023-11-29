cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project(client)

#set(Torch_DIR /home/xian/libtorch17/libtorch/share/cmake/Torch)
#find_package(Torch REQUIRED)
#find_package(OpenCV REQUIRED)
#include_directories("${OpenCV_INCLUDE_DIRS}")

add_executable(client client.cpp)
#target_link_libraries(client "${TORCH_LIBRARIES}")
#target_link_libraries(client ${OpenCV_LIBS})
target_link_libraries(client /usr/local/lib/libwiredtiger.so)

set_property(TARGET client PROPERTY CXX_STANDARD 14)
