import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append("../..")
from lpbound.cpp_solver.lpbound_parallel import create_input_files as cf
import subprocess
from lpbound.config.paths import LpBoundPaths
from src.lpbound.LpFlow.flow_bound_wrapper import  py_flow_bound
import csv
import shutil


def run_LpFlow():
    path = f"{LpBoundPaths.PROJ_ROOT_DIR}/data/lps/jobjoin_lpberge"
    lps = os.listdir(path)
    lps = [lp for lp in lps if lp.endswith(".lp")]
    
    # Open CSV file for writing
    output_file = f"{LpBoundPaths.PROJ_ROOT_DIR}/results/estimation_time/flow_bound_timing_results.csv"
    with open(output_file, 'w+', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['query', 'build_time', 'solve_time', 'objective'])
    
    for lp in lps:
        with open(path + "/" + lp) as f:
            lines = [line.rstrip() for line in f]
        query_id = lp.split("_")[-1].split(".")[0]
        dict_stat_ineqs = {}
        
        dcs = []
        var_set = set()
        
        for l in lines:
            if l.strip().startswith("Statistics_Ineq."):
                eq = l.split(":")[1].split("<=")[0].strip()
                rhs = float(l.split("<=")[1].strip())
                p = l.split("_|_")[1]  
                vars = [v for v in eq.split(" ") if v.startswith("_")]
                vars = [v.strip("_") for v in vars]
                
                if len(vars) == 1:
                    join_var = ()
                    data_vars_query = vars[0].split("_")
                    p = 1
                    
                    dc = ((), tuple(data_vars_query), p, rhs/p)
                    
                    for v in data_vars_query:
                        var_set.add(v)
                        
                elif p == "inf":
                    join_var = vars[0]
                    data_var = vars[1].split("_")
                    
                    p = float("inf")
                    dc = ((join_var), tuple(data_vars_query), p, rhs)
                    
                    for v in data_vars_query:
                        var_set.add(v)
                    
                else:
                    join_var = vars[0]
                    data_var = vars[1].split("_")
                    
                    p = float(p)
                    dc = ((join_var), tuple(data_vars_query), p, rhs/p)
                    
                    for v in data_vars_query:
                        var_set.add(v)
                    
                dcs.append(dc)
            
        var_list = list(var_set) 
        res = py_flow_bound(dcs, var_list)
        
        # Append results to CSV file
        with open(output_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([query_id, res[1], res[2], res[0]])
    
    

def main():
    
    # creating input files:
    print("Creating input files...")
    #cf.main()
    
    # run LpFlow
    #run_LpFlow()    
    
    # run LpBerge
    lp_berge_script = "./run_lpberge.sh"
    
    print("\nRunning LpBerge...")
    print(f"Running script: {lp_berge_script}")
    try:
        result_berge = subprocess.run(lp_berge_script, shell=True, capture_output=True, text=True, check=True, cwd="src/lpbound/cpp_solver/lpbound_parallel/")
        print("LpBerge stdout:", result_berge.stdout)
        if result_berge.stderr:
            print("LpBerge stderr:", result_berge.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error running LpBerge: {e}")
        print(f"Return code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        sys.exit(1)
    
    # copy output to results
    source_file = f"{LpBoundPaths.PROJ_ROOT_DIR}/src/lpbound/cpp_solver/lpbound_parallel/output/runtimes_jobjoin_lpbound_optimized_full_queries.txt"
    target_file = f"{LpBoundPaths.PROJ_ROOT_DIR}/results/estimation_time/runtimes_optimized_lpbound.txt"
    
    # Copy the source file to target location (overwrite if exists)
    try:
        shutil.copy2(source_file, target_file)
        print(f"Successfully copied {source_file} to {target_file}")
    except FileNotFoundError:
        print(f"Error: Source file {source_file} not found")
    except Exception as e:
        print(f"Error copying file: {e}")
    
    # run LpBase
    lp_base_script = "./run_lpbase.sh"
    print("Warning: The following experiment takes several hours to complete.")
    
    print("\nRunning LpBase...")
    print(f"Running script: {lp_base_script}")
    try:
        result_base = subprocess.run(lp_base_script, shell=True, capture_output=True, text=True, check=True, cwd="src/lpbound/cpp_solver/lpbound_parallel/")
        print("LpBase stdout:", result_base.stdout)
        if result_base.stderr:
            print("LpBase stderr:", result_base.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error running LpBase: {e}")
        print(f"Return code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        sys.exit(1)
        
        # copy output to results
    source_file = f"{LpBoundPaths.PROJ_ROOT_DIR}/src/lpbound/cpp_solver/lpbound_parallel/output/runtimes_jobjoin_lpbound_optimized_full_queries.txt"
    target_file = f"{LpBoundPaths.PROJ_ROOT_DIR}/results/estimation_time/runtimes_elemental_shannon_lpbound_agg.csv"
    try:
        shutil.copy2(source_file, target_file)
        print(f"Successfully copied {source_file} to {target_file}")
    except FileNotFoundError:
        print(f"Error: Source file {source_file} not found")
    except Exception as e:
        print(f"Error copying file: {e}")
    
    print("\nAll processes completed successfully!")

if __name__ == "__main__":
    main()
