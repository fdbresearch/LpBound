from setuptools import setup, Extension
from Cython.Build import cythonize
import os
import numpy as np

# Get the absolute path to the directory containing setup.py
current_dir = os.path.dirname(os.path.abspath(__file__))
# Get the path to HiGHS relative to the current directory
highs_dir = os.path.abspath(os.path.join(current_dir, "..", "HiGHS"))

# Define additional includes and library dirs
include_dirs = [
    current_dir,
    os.path.join(highs_dir, "src"),
    os.path.join(highs_dir, "build"),
    os.path.join(highs_dir, "extern")
]

library_dirs = [
    os.path.join(highs_dir, "build", "lib"),
    os.path.join(highs_dir, "build")
]

# RPATH settings for runtime library resolution
rpath = [
    os.path.join(highs_dir, "build", "lib"),
    os.path.join(highs_dir, "build")
]

# Convert relative paths to absolute for RPATH
rpath = [os.path.abspath(p) for p in rpath]

# Define the extension
extension = Extension(
    "lp_construction_wrapper",
    sources=["lp_construction_wrapper.pyx", "lp_construction.cpp"],
    include_dirs=include_dirs,
    library_dirs=library_dirs,
    libraries=["highs", "m", "stdc++"],
    extra_compile_args=["-std=c++14", "-O3"],  # Using C++14 instead of C++11
    extra_link_args=[f"-Wl,-rpath,{p}" for p in rpath],  # Add RPATH to the shared library
    language="c++"
)

setup(
    ext_modules=cythonize([extension], compiler_directives={'language_level': "3"}),
    zip_safe=False,
) 