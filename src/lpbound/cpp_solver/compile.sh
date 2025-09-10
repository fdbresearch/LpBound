#!/bin/bash

# Define the build directory
BUILD_DIR="build"

# Check if the build directory exists and remove it
if [ -d "$BUILD_DIR" ]; then
    echo "Removing existing build directory..."
    rm -rf "$BUILD_DIR"
fi

# Create a new build directory
echo "Creating new build directory..."
mkdir -p "$BUILD_DIR"

# Change to the build directory
cd "$BUILD_DIR"

# Configure the project with CMake
echo "Configuring project with CMake..."
cmake ..

# Build the project with Make
echo "Building project..."
make

# Check if the build was successful
if [ $? -eq 0 ]; then
    echo "Build successful!"
    
    # Run the test program
    echo -e "\nRunning powerset test...\n"
    ./test_powerset
else
    echo "Build failed!"
    exit 1
fi

echo "Script completed." 