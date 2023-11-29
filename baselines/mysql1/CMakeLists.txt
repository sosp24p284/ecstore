cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project(client)

#set(Torch_DIR /home/xian/libtorch17/libtorch/share/cmake/Torch)
#find_package(Torch REQUIRED)
#find_package(OpenCV REQUIRED)
#include_directories("${OpenCV_INCLUDE_DIRS}")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
add_executable(client client.cpp)

include_directories(/usr/include/mysql)

#find_path(MYSQL_ROOT_DIR mysql)
#include(FindPackageHandleStandardArgs)
#find_package_handle_standard_args(mysql REQUIRED_VARS MYSQL_ROOT_DIR)

#add_executable(${PROJECT_NAME} client.cpp)
#target_include_directories(${PROJECT_NAME} PRIVATE ${MYSQL_ROOT_DIR}/include)
#target_link_directories(${PROJECT_NAME} PRIVATE ${MYSQL_ROOT_DIR}/lib)
#target_link_libraries(${PROJECT_NAME} libmysql)

#target_link_libraries(server "${TORCH_LIBRARIES}")
#target_link_libraries(server ${OpenCV_LIBS})
#target_link_libraries(wiredtiger /usr/local/lib/libwiredtiger.so)
#add_compile_options(-pthread)
target_link_libraries(client -lmysqlclient)
set_property(TARGET client PROPERTY CXX_STANDARD 14)


#include_directories(/usr/include/mysql)
#add_executable(client client.cpp)