# distutils: language = c++
# distutils: sources = flow_bound.cpp

from libcpp.vector cimport vector
from libcpp.set cimport set
from libcpp cimport bool
from libc.string cimport const_char

cdef extern from "<string>" namespace "std":
    cdef cppclass string:
        string()
        string(const char*)
        const char* c_str()

cdef extern from "flow_bound.h":
    cdef cppclass DC[T]:
        set[T] X
        set[T] Y
        double p
        double b
        DC(const set[T]& X_, const set[T]& Y_, double p_, double b_)

    vector[double] flow_bound(
        const vector[DC[string]]& dcs,
        const vector[string]& target_vars,
        bool use_only_chain_bound,
        bool use_weighted_edges
    )

cdef inline string py_str_to_cpp_str(s):
    if isinstance(s, str):
        return string(s.encode('utf-8'))
    elif hasattr(s, 'name'):  # Assuming OR-Tools variables have a 'name' attribute
        return string(s.name().encode('utf-8'))
    else:
        return string(str(s).encode('utf-8'))

def py_flow_bound(list edges, list target_vars, bool use_only_chain_bound=False, bool use_weighted_edges=True):
    cdef vector[DC[string]] dcs_vec
    cdef vector[string] target_vars_vec
    cdef set[string] X, Y
    cdef string x_str, y_el_str
    
    # Convert Python lists to C++ vectors
    for edge in edges:
        X.clear()
        Y.clear()
        
        x_str = py_str_to_cpp_str(edge[0])
        X.insert(x_str)
        
        for y_el in edge[1]:
            y_el_str = py_str_to_cpp_str(y_el)
            Y.insert(y_el_str)
        
        # Create DC instance and add it to the vector
        dcs_vec.push_back(DC[string](X, Y, float(edge[2]), float(edge[3])))

    for var in target_vars:
        target_vars_vec.push_back(py_str_to_cpp_str(var))
    
    # Call the C++ function
    return flow_bound(dcs_vec, target_vars_vec, use_only_chain_bound, use_weighted_edges)