import subprocess
import os
import sys
# add src directory to sys path to find the lpbound module
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from lpbound.config.paths import LpBoundPaths

def run_lpbound_cpp(run_parallel):
    # Path to the C++ executable using LpBoundPaths
    executable_path = LpBoundPaths.PROJ_ROOT_DIR / "src/lpbound/cpp_solver/lpbound_parallel/build/lpbound_parallel"
    
    # Calculate absolute paths for input and output directories
    input_data_dir = LpBoundPaths.PROJ_ROOT_DIR / "src/lpbound/cpp_solver/lpbound_parallel/input_data"
    output_dir = LpBoundPaths.RESULTS_DIR / "estimation_time" / "raw_results"
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Prepare command arguments
    cmd = [
        str(executable_path),
        "--input-dir", str(input_data_dir),
        "--output-dir", str(output_dir)
    ]
    
    if run_parallel:
        cmd.append("--parallel")
    else:
        cmd.append("--sequential")
    
    # We can run from any directory now since we're using absolute paths
    working_directory = LpBoundPaths.PROJ_ROOT_DIR
    
    try:
        print(f"Running: {' '.join(cmd)}")
        print(f"Working directory: {working_directory}")
        print(f"Input data directory: {input_data_dir}")
        print(f"Output directory: {output_dir}")
        
        result = subprocess.run(
            cmd,
            cwd=str(working_directory),
            capture_output=True,
            text=True,
            check=True
        )
        
        print("Program completed successfully!")
        if result.stdout:
            print("Program output:", result.stdout)
        if result.stderr:
            print("Program stderr:", result.stderr)
            
    except subprocess.CalledProcessError as e:
        print("Error running the program:", e)
        print("Return code:", e.returncode)
        if e.stdout:
            print("Program output:", e.stdout)
        if e.stderr:
            print("Program error:", e.stderr)
    except FileNotFoundError:
        print(f"Error: Executable not found at {executable_path}")
        print("Make sure you have compiled the C++ program first.")
        print("Run: cd src/lpbound/cpp_solver/lpbound_parallel && bash compile.sh")


def aggregate_results():
    # TODO: Implement results aggregation logic
    pass


if __name__ == "__main__":
    
    #run_lpbound_cpp(
    #    run_parallel=False
    #)
    
    run_lpbound_cpp(
        run_parallel=True
    )
