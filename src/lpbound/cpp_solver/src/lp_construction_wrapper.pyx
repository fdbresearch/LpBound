# cython: language_level=3
from libcpp.vector cimport vector
from libcpp.map cimport map
from libcpp.pair cimport pair
from cython.operator cimport dereference as deref

# Define the nested pair types correctly
ctypedef pair[int, int] int_pair_t
ctypedef pair[int_pair_t, vector[double]] stats_entry_t

cdef extern from "lp_construction.h":
    double elemental_shannon_lp(
        const vector[int]& all_vars,
        const map[pair[int, int], vector[double]]& statistics
    )

def py_elemental_shannon_lp(list all_vars, dict dict_statistics):
    cdef vector[int] c_all_vars
    cdef map[pair[int, int], vector[double]] c_statistics
    cdef pair[int, int] key_pair
    cdef vector[double] values_vector
    cdef int var, i
    cdef double v
    
    # Convert Python list to C++ vector
    for var in all_vars:
        c_all_vars.push_back(var)
    
    # Convert Python dict to C++ map
    for key, value in dict_statistics.items():
        # Create the pair key
        key_pair = pair[int, int](key[0], key[1])
        
        # Create the vector of doubles for values
        values_vector.clear()  # Clear any previous values
        for v in value:
            values_vector.push_back(v)
            
        c_statistics[key_pair] = values_vector
    
    return elemental_shannon_lp(c_all_vars, c_statistics) 