cmake_minimum_required(VERSION 3.5)

project(olcbtree LANGUAGES CXX)

add_compile_options(-std=c++11 -g)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(olcbtree
    ../node.h
    ../gc.h
    ../gc.cpp
    ../btree.h
    main.cpp)
target_link_libraries(olcbtree pthread gtest)
