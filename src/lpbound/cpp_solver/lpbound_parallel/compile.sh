#!/bin/bash

# Enhanced build script for maximum performance
# Usage: ./compile.sh [--ninja] [--pgo] [--jobs N]

# Parse command line arguments
USE_NINJA=false
USE_PGO=false
JOBS=$(nproc)  # Use all available CPU cores by default

while [[ $# -gt 0 ]]; do
    case $1 in
        --ninja)
            USE_NINJA=true
            shift
            ;;
        --pgo)
            USE_PGO=true
            shift
            ;;
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--ninja] [--pgo] [--jobs N]"
            exit 1
            ;;
    esac
done

# Define the build directory
BUILD_DIR="build"

# Check if the build directory exists and remove it
if [ -d "$BUILD_DIR" ]; then
    echo "Removing existing build directory..."
    rm -rf "$BUILD_DIR"
fi

# Create a new build directory
echo "Creating new build directory..."
mkdir "$BUILD_DIR"

# Change to the build directory
cd "$BUILD_DIR"

# Configure build system
if [ "$USE_NINJA" = true ]; then
    echo "Configuring project with CMake (Ninja generator)..."
    CMAKE_GENERATOR="-G Ninja"
    BUILD_COMMAND="ninja -j$JOBS"
else
    echo "Configuring project with CMake (Make generator)..."
    CMAKE_GENERATOR=""
    BUILD_COMMAND="make -j$JOBS"
fi

# Configure CMAKE options
CMAKE_OPTIONS="-DCMAKE_BUILD_TYPE=Release"
if [ "$USE_PGO" = true ]; then
    CMAKE_OPTIONS="$CMAKE_OPTIONS -DENABLE_PGO=ON"
fi

if [ "$USE_PGO" = true ]; then
    echo "=== Profile-Guided Optimization (PGO) Build ==="
    
    # Step 1: Configure and build with profile generation
    echo "Step 1: Building with profile generation..."
    cmake $CMAKE_GENERATOR $CMAKE_OPTIONS ..
    
    if [ $? -ne 0 ]; then
        echo "CMake configuration for PGO failed!"
        exit 1
    fi
    
    $BUILD_COMMAND
    
    if [ $? -ne 0 ]; then
        echo "Profile generation build failed!"
        exit 1
    fi
    
    # Step 2: Run the executable to generate profile data
    echo "Step 2: Running executable to generate profile data..."
    echo "Please run your typical workload now to generate profile data."
    echo "Example: ./lpbound_parallel --sequential --input-dir ../../../test_data --output-dir ./profile_output"
    echo "Press Enter when you've finished running the profiling workload..."
    read -r
    
    # Step 3: Rebuild with profile-guided optimization  
    echo "Step 3: Rebuilding with profile-guided optimization..."
    
    # Clean and reconfigure for profile use
    cd ..
    rm -rf "$BUILD_DIR"
    mkdir "$BUILD_DIR"
    cd "$BUILD_DIR"
    
    # Use profile data for optimization
    cmake $CMAKE_GENERATOR -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS=-fprofile-use ..
    $BUILD_COMMAND
    
    if [ $? -eq 0 ]; then
        echo "PGO build successful!"
    else
        echo "PGO build failed!"
        exit 1
    fi
    
else
    # Standard optimized build
    echo "Building project with maximum optimization..."
    cmake $CMAKE_GENERATOR $CMAKE_OPTIONS ..
    
    # Check if configuration was successful
    if [ $? -ne 0 ]; then
        echo "CMake configuration failed!"
        exit 1
    fi
    
    echo "Building with $JOBS parallel jobs..."
    $BUILD_COMMAND
    
    # Check if the build was successful
    if [ $? -eq 0 ]; then
        echo "Build successful!"
    else
        echo "Build failed!"
        exit 1
    fi
fi

echo "=== Build Summary ==="
echo "Generator: $(if [ "$USE_NINJA" = true ]; then echo "Ninja"; else echo "Make"; fi)"
echo "Parallel jobs: $JOBS"
echo "PGO enabled: $USE_PGO"
echo "Executable: ./lpbound_parallel"
echo ""
echo "To test the executable:"
echo "  ./lpbound_parallel --help"
echo "Script completed."
