cmake_minimum_required(VERSION 3.10)

project(MyProject VERSION 1.0)

# Specify the C++ standard
set(CMAKE_CXX_STANDARD 17)  # Use C++17 for potential performance improvements
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Enable Link Time Optimization
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)

# Option for Profile-Guided Optimization
option(ENABLE_PGO "Enable Profile-Guided Optimization" OFF)

# Detect CPU architecture for optimal flags
include(CheckCXXCompilerFlag)

# Base optimization flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -march=native -mtune=native")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ffast-math -funroll-loops -flto")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG")

# Advanced CPU optimizations (check if supported)
check_cxx_compiler_flag("-mavx2" COMPILER_SUPPORTS_AVX2)
if(COMPILER_SUPPORTS_AVX2)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -mfma")
endif()

check_cxx_compiler_flag("-mbmi2" COMPILER_SUPPORTS_BMI2)
if(COMPILER_SUPPORTS_BMI2)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mbmi2 -mpopcnt")
endif()

# Memory and cache optimizations
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -falign-functions=32 -falign-loops=32")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprefetch-loop-arrays")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-stack-protector")

# Aggressive inlining and optimization
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -finline-functions -finline-limit=1000")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fomit-frame-pointer")

# Remove problematic flags that might not be supported everywhere
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions -fno-rtti")

# Link-time optimizations
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Ofast -flto")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gc-sections")

# PGO flags (only if enabled)
if(ENABLE_PGO)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-generate")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lgcov")
endif()

# Find OpenMP package
find_package(OpenMP REQUIRED)

# Include directories
include_directories(
    ../HiGHS/highs
    ../HiGHS/build
    ../json/include
)

# Link directories
link_directories(../HiGHS/build/lib)

# Add executable
add_executable(lpbound_parallel src/lpbound_parallel.cpp)

# Link libraries
target_link_libraries(lpbound_parallel 
    PUBLIC 
    OpenMP::OpenMP_CXX
    highs
    m
    stdc++
)

# Set additional target properties for maximum optimization
set_target_properties(lpbound_parallel PROPERTIES 
    LINK_FLAGS "-Ofast -march=native -mtune=native -flto -Wl,--gc-sections"
)