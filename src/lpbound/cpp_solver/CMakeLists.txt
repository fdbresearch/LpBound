cmake_minimum_required(VERSION 3.10)

project(CPPSolver VERSION 1.0)

# Specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Enable optimizations
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

# Set the path to HiGHS based on your provided location
set(HIGHS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/HiGHS" CACHE PATH "Path to HiGHS library")

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${HIGHS_DIR}/src
    ${HIGHS_DIR}/build
)

# Link directories
link_directories(${HIGHS_DIR}/build/lib)

# Add test executable
add_executable(test_powerset src/test_powerset.cpp)

# Link libraries
target_link_libraries(test_powerset
    highs
    m
    stdc++
) 