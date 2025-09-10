
import pandas as pd
import json
import os
import itertools
from itertools import *
import sys
import importlib    
import lpbound.cpp_solver.lpbound_parallel.constraints as u
from lpbound.config.paths import LpBoundPaths
from lpbound.cpp_solver.lpbound_parallel.create_input_files import process_subqueries


def main():
    
    print("Processing JOBjoin")
    
    process_subqueries(
        "jobjoin", 33, [29, 31]
    )
    
    print("Processing JOBlight")
    process_subqueries(
        "joblight", 70, []
    )
    
    print("Processing STATS")
    process_subqueries(
        "stats", 146, []
    )
    
if __name__ == "__main__":
    main()
