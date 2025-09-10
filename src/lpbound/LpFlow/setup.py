from setuptools import setup, Extension, find_packages
from Cython.Build import cythonize
import os
import numpy as np

# Paths - convert to absolute paths
base_dir = os.path.dirname(os.path.abspath(__file__))
highs_root_dir = os.path.abspath(os.path.join(base_dir, '../cpp_solver/HiGHS'))
highs_build_dir = os.path.abspath(os.path.join(base_dir, '../cpp_solver/HiGHS/build'))
highs_lib_dir = os.path.abspath(os.path.join(highs_build_dir, 'lib'))

# Find the HiGHS library file
highs_lib_file = 'libhighs.so'
if not os.path.exists(os.path.join(highs_lib_dir, highs_lib_file)):
    raise FileNotFoundError(f"HiGHS library not found in {highs_lib_dir}")

print(f"Found HiGHS library: {highs_lib_file}")

# Define the extension
# Note: changed the name to include the full package path
extensions = [
    Extension(
        'flow_bound_wrapper',  # Keep it simple for in-place build
        sources=['flow_bound_wrapper.pyx'],  # Keep original source path
        include_dirs=[
            highs_root_dir,
            os.path.join(highs_root_dir, 'highs'),
            highs_build_dir,
            np.get_include(),
        ],
        library_dirs=[highs_lib_dir],
        libraries=['highs'],
        language='c++',
        extra_compile_args=['-std=c++11', '-fopenmp'],
        extra_link_args=['-fopenmp', f'-Wl,-rpath,{highs_lib_dir}'],
        runtime_library_dirs=[highs_lib_dir],
    )
]

# Setup
setup(
    name='LpBound',  # Changed to match package name
    packages=find_packages(),  # This will find all packages
    ext_modules=cythonize(extensions, language_level="3"),
    install_requires=['numpy'],
)