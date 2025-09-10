#include <fstream>
#include <iostream>
#include "Highs.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;
#include <vector>
#include <chrono>
#include <unordered_map>
#include <cstring>
#include <map>

std::vector<std::string> benchmarks = {"stats", "jobjoin", "joblight"};
//std::vector<std::string> benchmarks = {"joblight"};
std::map<std::string, int> benchmark_number_of_queries = {
    {"stats", 146},
    {"jobjoin", 33},
    {"joblight", 70}
};

// Thread-local storage for solver instances and memory buffers
thread_local Highs* tl_highs = nullptr;
thread_local std::vector<int> tl_start;
thread_local std::vector<int> tl_index;
thread_local std::vector<double> tl_values;
// Pre-allocated LP vectors to avoid repeated allocations
thread_local std::vector<double> tl_col_cost;
thread_local std::vector<double> tl_col_lower;
thread_local std::vector<double> tl_col_upper;
thread_local std::vector<double> tl_row_lower;
thread_local std::vector<double> tl_row_upper;
thread_local bool tl_initialized = false;

// Initialize thread-local solver and memory buffers
void initialize_thread_local_resources() {
    if (!tl_initialized) {
        tl_highs = new Highs();
        // Set solver options once per thread
        tl_highs->setOptionValue("presolve", "off");
        tl_highs->setOptionValue("output_flag", false);
        tl_highs->setOptionValue("log_to_console", false);
        tl_highs->setOptionValue("write_solution_to_file", false);
        tl_highs->setOptionValue("message_level", 0);
        
        // Pre-allocate memory buffers with reasonable capacity
        tl_start.reserve(1000);
        tl_index.reserve(10000);
        tl_values.reserve(10000);
        
        // Pre-allocate LP vectors
        tl_col_cost.reserve(1000);
        tl_col_lower.reserve(1000);
        tl_col_upper.reserve(1000);
        tl_row_lower.reserve(1000);
        tl_row_upper.reserve(1000);
        
        tl_initialized = true;
    }
}

// Cleanup function for thread-local resources
void cleanup_thread_local_resources() {
    if (tl_highs) {
        delete tl_highs;
        tl_highs = nullptr;
    }
    tl_initialized = false;
}

std::vector<double> naive_build_and_solve(
    const std::vector<std::unordered_map<int, double>>& submod_monoton_constraints,
    const std::vector<std::unordered_map<int, double>>& stat_constr,
    const std::unordered_map<int, std::string>& variables,
    const std::unordered_map<int, int>& objective_function,
    int query_id,
    int run_id) {


    // start build time
    std::chrono::steady_clock::time_point begin_build = std::chrono::steady_clock::now();

    // Ensure thread-local resources are initialized
    initialize_thread_local_resources();

    int num_vars = variables.size();
    int num_submod_monoton = submod_monoton_constraints.size();
    int num_stat_constr = stat_constr.size();
    int total_rows = num_submod_monoton + num_stat_constr;

    // Pre-calculate total nonzeros to reserve exact capacity
    int estimated_nonzeros = 0;
    for (const auto& constraint : submod_monoton_constraints) {
        estimated_nonzeros += constraint.size();
    }
    for (const auto& constraint : stat_constr) {
        estimated_nonzeros += constraint.size() - 1; // -1 for RHS key
    }

    // Clear and reuse existing model
    tl_highs->clearModel();
    
    HighsLp lp;
    
    lp.num_col_ = num_vars;
    lp.num_row_ = total_rows;

    lp.offset_ = 0.0;
    
    // Use pre-allocated vectors instead of assign() - much faster
    // set cost: objective_function map: keys are indices and values are coefficients

    tl_col_cost.clear();
    tl_col_cost.resize(num_vars);
    std::fill(tl_col_cost.begin(), tl_col_cost.end(), 0.0);
    for (const auto& [var_id, coeff] : objective_function) {
        if (var_id >= 0 && var_id < num_vars) {
            tl_col_cost[var_id] = coeff;
        } else {
            std::cerr << "Warning: Invalid variable ID " << var_id << " in objective function." << std::endl;
        }
    }
    lp.col_cost_ = tl_col_cost;

    lp.sense_ = ObjSense::kMaximize;

    // Use pre-allocated vectors for bounds
    tl_col_lower.resize(num_vars);
    tl_col_upper.resize(num_vars);
    tl_row_lower.resize(total_rows);
    tl_row_upper.resize(total_rows);
    
    std::fill(tl_col_lower.begin(), tl_col_lower.end(), 0.0);
    std::fill(tl_col_upper.begin(), tl_col_upper.end(), 1e30);
    std::fill(tl_row_lower.begin(), tl_row_lower.end(), 0.0);
    std::fill(tl_row_upper.begin(), tl_row_upper.end(), 1e30);
    
    lp.col_lower_ = tl_col_lower;
    lp.col_upper_ = tl_col_upper;
    lp.row_lower_ = tl_row_lower;
    lp.row_upper_ = tl_row_upper;

    // Reserve exact capacity to avoid reallocations during construction
    tl_start.clear();
    tl_index.clear();
    tl_values.clear();
    tl_start.reserve(total_rows + 1);
    tl_index.reserve(estimated_nonzeros);
    tl_values.reserve(estimated_nonzeros);

    lp.a_matrix_.format_ = MatrixFormat::kRowwise;
    tl_start.push_back(0);
    int total_nonzeros = 0;

    // Process submodular monotonicity constraints
    for (const auto& constraint : submod_monoton_constraints) {
        // Reserve space for this constraint
        tl_index.reserve(tl_index.size() + constraint.size());
        tl_values.reserve(tl_values.size() + constraint.size());
        
        for (const auto& [var_id, coeff] : constraint) {
            tl_index.push_back(var_id);
            tl_values.push_back(coeff);
            total_nonzeros++;
        }
        tl_start.push_back(total_nonzeros);
    }

    // Process statistical constraints
    for (size_t i = 0; i < num_stat_constr; ++i) {
        const auto& constraint = stat_constr[i];
        lp.row_upper_[num_submod_monoton + i] = constraint.at(-1);  // RHS
        
        // Reserve space for this constraint (excluding RHS)
        int constraint_size = constraint.size() - 1;
        tl_index.reserve(tl_index.size() + constraint_size);
        tl_values.reserve(tl_values.size() + constraint_size);
        
        for (const auto& [var_id, coeff] : constraint) {
            if (var_id != -1) {
                tl_index.push_back(var_id);
                tl_values.push_back(coeff);
                total_nonzeros++;
            }
        }
        tl_start.push_back(total_nonzeros);
    }

    lp.a_matrix_.start_ = tl_start;
    lp.a_matrix_.index_ = tl_index;
    lp.a_matrix_.value_ = tl_values;

    tl_highs->passModel(lp);

    // end build time
    std::chrono::steady_clock::time_point end_build = std::chrono::steady_clock::now();
    int build_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end_build - begin_build).count();

    // start solve time
    std::chrono::steady_clock::time_point begin_solve = std::chrono::steady_clock::now();

    HighsStatus solveStatus = tl_highs->run();

    const HighsInfo& info = tl_highs->getInfo();

    // end solve time
    std::chrono::steady_clock::time_point end_solve = std::chrono::steady_clock::now();
    int solve_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end_solve - begin_solve).count();

    // return solve time + build time
    std::vector<double> result;
    result.push_back(info.objective_function_value);
    result.push_back(build_time);
    result.push_back(solve_time);
    return result;
}

int main(int argc, char* argv[]) {
    enum ExecutionMode { SEQUENTIAL, PARALLEL, FULL_QUERIES };
    ExecutionMode exec_mode = SEQUENTIAL;  // default
    std::string input_data_dir = "./input_data";  // default
    std::string output_dir = "./output";          // default
    bool input_dir_specified = false;
    bool output_dir_specified = false;
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--parallel") == 0 || strcmp(argv[i], "-p") == 0) {
            exec_mode = PARALLEL;
        } else if (strcmp(argv[i], "--sequential") == 0 || strcmp(argv[i], "-s") == 0) {
            exec_mode = SEQUENTIAL;
        } else if (strcmp(argv[i], "--full-queries") == 0 || strcmp(argv[i], "-f") == 0) {
            exec_mode = FULL_QUERIES;
        } else if (strcmp(argv[i], "--input-dir") == 0) {
            if (i + 1 < argc) {
                input_data_dir = argv[++i];
                input_dir_specified = true;
            } else {
                std::cerr << "Error: --input-dir requires a directory path" << std::endl;
                return 1;
            }
        } else if (strcmp(argv[i], "--output-dir") == 0) {
            if (i + 1 < argc) {
                output_dir = argv[++i];
                output_dir_specified = true;
            } else {
                std::cerr << "Error: --output-dir requires a directory path" << std::endl;
                return 1;
            }
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            std::cout << "Usage: " << argv[0] << " [OPTIONS]" << std::endl;
            std::cout << "Options:" << std::endl;
            std::cout << "  --parallel, -p          : Run in parallel mode" << std::endl;
            std::cout << "  --sequential, -s        : Run in sequential mode (default)" << std::endl;
            std::cout << "  --full-queries, -f      : Run in full-queries mode (requires --input-dir and --output-dir)" << std::endl;
            std::cout << "  --input-dir <path>      : Input data directory path (required for full-queries mode)" << std::endl;
            std::cout << "  --output-dir <path>     : Output directory path (required for full-queries mode)" << std::endl;
            std::cout << "  --help, -h              : Show this help message" << std::endl;
            return 0;
        } else {
            std::cerr << "Unknown argument: " << argv[i] << std::endl;
            std::cerr << "Use --help for usage information." << std::endl;
            return 1;
        }
    }
    
    // Validate required arguments for full-queries mode
    if (exec_mode == FULL_QUERIES) {
        if (!input_dir_specified || !output_dir_specified) {
            std::cerr << "Error: --full-queries mode requires both --input-dir and --output-dir to be specified" << std::endl;
            std::cerr << "Use --help for usage information." << std::endl;
            return 1;
        }
    }
    
    // Print execution mode
    std::string mode_str = (exec_mode == PARALLEL) ? "parallel" : 
                           (exec_mode == FULL_QUERIES) ? "full-queries" : "sequential";
    std::cout << "Running in " << mode_str << " mode" << std::endl;
    std::cout << "Input data directory: " << input_data_dir << std::endl;
    std::cout << "Output directory: " << output_dir << std::endl;

    int n_runs = 5;

    for (const auto& benchmark : benchmarks) {
        int n_queries = benchmark_number_of_queries[benchmark];

        std::string outputfile;
        if (exec_mode == PARALLEL) {
            outputfile = output_dir + "/runtimes_" + benchmark + "_lpbound_optimized_full_lattice_parallel.txt";
        } else if (exec_mode == FULL_QUERIES) {
            outputfile = output_dir + "/runtimes_" + benchmark + "_lpbound_optimized_full_queries.txt";
        } else {
            outputfile = output_dir + "/runtimes_" + benchmark + "_lpbound_optimized_full_lattice_sequential.txt";
        }

        std::ofstream out(outputfile);

        if (exec_mode == PARALLEL) {
            out << "query_id,run_id,total_time" << std::endl;
        } else if (exec_mode == FULL_QUERIES) {
            out << "query_id,run_id,build_time,solve_time,estimate" << std::endl;
        } else {
            out << "query_id,run_id,lattice,runtime,estimate" << std::endl;
        }

        std::cout << "Starting to construct the lp models ..." << std::endl;
        
        // Skip data loading for full-queries mode (will load per query)
        if (exec_mode == FULL_QUERIES) {
            // Data will be loaded per query in the execution loop
        } else {
            std::cout << "Loading the data." << std::endl;
        }
        
        // Pre-load all data (only for parallel and sequential modes)
        std::vector<int> num_lattice_points(n_queries+1);
        std::vector<std::vector<std::vector<std::unordered_map<int, double>>>> all_submod_monoton_constraints(n_queries+1);
        std::vector<std::vector<std::vector<std::unordered_map<int, double>>>> all_stat_constr(n_queries+1);
        std::vector<std::vector<std::unordered_map<int, std::string>>> all_variables(n_queries+1);
        std::vector<std::vector<std::unordered_map<int, int>>> all_objective_functions(n_queries+1);
        
        if (exec_mode != FULL_QUERIES) {
            for (int i = 1; i <= n_queries; i++) {
            std::cout << "Loading query " << i << std::endl;
            
            std::string query_id = std::to_string(i);
            // jobjoin queries 29 and 31 were excluded from other experiments
            if (benchmark == "jobjoin" && (i == 29 || i == 31)) {
                continue;
            }

            std::string filenameLattice = input_data_dir + "/" + benchmark + "/lattice_points_" + query_id + ".txt";
            std::ifstream file(filenameLattice);
            file >> num_lattice_points[i];
            file.close();

            all_submod_monoton_constraints[i].resize(num_lattice_points[i]);
            all_stat_constr[i].resize(num_lattice_points[i]);
            all_variables[i].resize(num_lattice_points[i]);
            all_objective_functions[i].resize(num_lattice_points[i]);
        
            for (int l = 0; l < num_lattice_points[i]; ++l) {
                std::string filenameSubmodMonoton = input_data_dir + "/" + benchmark + "/shannon_optimized_" + query_id + "_subq_" + std::to_string(l) + ".json";
                std::string filenameStatConstr = input_data_dir + "/" + benchmark + "/query_stats_ineq_" + query_id + "_subq_" + std::to_string(l) + ".json";
                std::string filenameVariables = input_data_dir + "/" + benchmark + "/query_vars_" + query_id + "_subq_" + std::to_string(l) + ".json";
                std::string filenameObjectiveFunction = input_data_dir + "/" + benchmark +  "/objective_function_" + query_id + "_subq_" + std::to_string(l) + ".json";

                std::ifstream f_submod_monoton(filenameSubmodMonoton);
                std::ifstream f_stat_constr(filenameStatConstr);
                std::ifstream f_variables(filenameVariables);
                std::ifstream f_objective_function(filenameObjectiveFunction);

                // std::cout << "Parsing shannon" << std::endl;
                json submod_monoton_constraints_q = json::parse(f_submod_monoton);
                // std::cout << "Parsing statistics" << std::endl;
                json stat_constr_q = json::parse(f_stat_constr);
                // std::cout << "Parsing variables" << std::endl;
                json variables_q = json::parse(f_variables);
                // std::cout << "Parsing objective function" << std::endl;
                json objective_function_q = json::parse(f_objective_function);

                // Conver objective function to unordered_map and store in all_objective_functions
                for (const auto& [key, value] : objective_function_q.items()) {
                    all_objective_functions[i][l][std::stoi(key)] = value.get<int>();
                }

                // Convert json to unordered_map
                for (const auto& constraint : submod_monoton_constraints_q) {
                    std::unordered_map<int, double> constraint_map;
                    for (const auto& [key, value] : constraint["variables"].items()) {
                        constraint_map[std::stoi(key)] = value.get<double>();
                    }
                    all_submod_monoton_constraints[i][l].push_back(constraint_map);
                }

                for (const auto& constraint : stat_constr_q) {
                    std::unordered_map<int, double> constraint_map;
                    for (const auto& [key, value] : constraint["variables"].items()) {
                        constraint_map[std::stoi(key)] = value.get<double>();
                    }
                    constraint_map[-1] = constraint["rhs"].get<double>();  // Store RHS with key -1
                    all_stat_constr[i][l].push_back(constraint_map);
                }

                for (const auto& [key, value] : variables_q.items()) {
                    all_variables[i][l][std::stoi(key)] = value.get<std::string>();
                }
            }
        }
            std::cout << "Finished loading data" << std::endl;
        }

        if (exec_mode == PARALLEL) {
            std::cout << "Running full lattice in parallel" << std::endl;
            // open new file for parallel results (estimates)
            //std::string outputfile_estimates = output_dir + "/estimates_" + benchmark + "_lpbound_optimized_full_lattice_parallel.txt";
            //std::ofstream out_estimates(outputfile_estimates);
            //out_estimates << "query_id,run_id,lattice,estimate" << std::endl;

            // Pre-initialize thread-local resources outside timing loops
            // #pragma omp parallel
            {
                initialize_thread_local_resources();
            }
            
            for (int i = 1; i <= n_queries; i++) {
                if (benchmark == "jobjoin" && (i == 29 || i == 31)) {
                    continue;
                }
                for (int j = 0; j < n_runs; j++) {
                    std::chrono::steady_clock::time_point begin_total = std::chrono::steady_clock::now();
                    double results = 0.0;
                    #pragma omp parallel for
                    for (int l = 0; l < num_lattice_points[i]; l++) {
                        
                        std::vector<double> results_vector = naive_build_and_solve(
                            all_submod_monoton_constraints[i][l],
                            all_stat_constr[i][l],
                            all_variables[i][l],
                            all_objective_functions[i][l],
                            i,
                            l
                        );
                        results = results_vector[0];
                        //out_estimates << i << "," << j << "," << l << "," << results << std::endl;
                        // std::cout << "Query: " << i << " Lattice: " << l << " Results: " << results << std::endl;
                    }

                    std::chrono::steady_clock::time_point end_total = std::chrono::steady_clock::now();
                    int total_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end_total - begin_total).count();

                    if (j != 0) {
                        out << i << "," << j << "," << total_time << std::endl;
                    }
                }
            }
        } else if (exec_mode == SEQUENTIAL) {
            std::cout << "Running full lattice sequentially" << std::endl;
            
            // Pre-initialize thread-local resources outside timing loops
            initialize_thread_local_resources();
            
            for (int i = 1; i <= n_queries; i++) {
                if (benchmark == "jobjoin" && (i == 29 || i == 31)) {
                    continue;
                }
                for (int j = 0; j < n_runs; j++) {
                    for (int l = 0; l < num_lattice_points[i]; l++) {
                        std::chrono::steady_clock::time_point begin_total = std::chrono::steady_clock::now();
                        std::vector<double> results_vector = naive_build_and_solve(
                            all_submod_monoton_constraints[i][l],
                            all_stat_constr[i][l],
                            all_variables[i][l],
                            all_objective_functions[i][l],
                            i,
                            l
                        );

                        std::chrono::steady_clock::time_point end_total = std::chrono::steady_clock::now();
                        double results = results_vector[0];
                        double build_time = results_vector[1];
                        double solve_time = results_vector[2];
                        int total_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end_total - begin_total).count();
                        if (j != 0) {
                            out << i << "," << j << "," << build_time << "," << solve_time << "," << results << std::endl;
                        }
                    }
                }
            }
        } else if (exec_mode == FULL_QUERIES) {
            // Full-queries mode - only for jobjoin benchmark
            //if (benchmark != "jobjoin") {
            //    std::cout << "Skipping " << benchmark << " - full-queries mode only supports jobjoin" << std::endl;
             //   continue;
            //}
            
            std::cout << "Running full queries mode" << std::endl;
            
            // Pre-initialize thread-local resources
            initialize_thread_local_resources();
            
            // Load data for full queries (no subqueries)
            for (int i = 1; i <= n_queries; i++) {
                //if (i == 29 || i == 31) {
                //    continue;  // Skip these queries
                //}
                
                std::string query_id = std::to_string(i);
                
                // Load single JSON files per query (no subquery suffix)
                std::string filenameSubmodMonoton = input_data_dir + "/shannon_optimized_" + query_id + ".json";
                std::string filenameStatConstr = input_data_dir + "/query_stats_ineq_" + query_id + ".json";
                std::string filenameVariables = input_data_dir + "/query_vars_" + query_id + ".json";
                std::string filenameObjectiveFunction = input_data_dir + "/objective_function_" + query_id + ".json";
                
                std::ifstream f_submod_monoton(filenameSubmodMonoton);
                std::ifstream f_stat_constr(filenameStatConstr);
                std::ifstream f_variables(filenameVariables);
                std::ifstream f_objective_function(filenameObjectiveFunction);
                
                if (!f_submod_monoton.is_open() || !f_stat_constr.is_open() || 
                    !f_variables.is_open() || !f_objective_function.is_open()) {
                    std::cerr << "Warning: Could not open files for query " << i << ", skipping" << std::endl;
                    continue;
                }
                
                json submod_monoton_constraints_q = json::parse(f_submod_monoton);
                json stat_constr_q = json::parse(f_stat_constr);
                json variables_q = json::parse(f_variables);
                json objective_function_q = json::parse(f_objective_function);
                
                // Convert to data structures
                std::vector<std::unordered_map<int, double>> submod_monoton_constraints;
                std::vector<std::unordered_map<int, double>> stat_constr;
                std::unordered_map<int, std::string> variables;
                std::unordered_map<int, int> objective_function;
                
                // Convert objective function
                for (const auto& [key, value] : objective_function_q.items()) {
                    objective_function[std::stoi(key)] = value.get<int>();
                }
                
                // Convert constraints
                for (const auto& constraint : submod_monoton_constraints_q) {
                    std::unordered_map<int, double> constraint_map;
                    for (const auto& [key, value] : constraint["variables"].items()) {
                        constraint_map[std::stoi(key)] = value.get<double>();
                    }
                    submod_monoton_constraints.push_back(constraint_map);
                }
                
                for (const auto& constraint : stat_constr_q) {
                    std::unordered_map<int, double> constraint_map;
                    for (const auto& [key, value] : constraint["variables"].items()) {
                        constraint_map[std::stoi(key)] = value.get<double>();
                    }
                    constraint_map[-1] = constraint["rhs"].get<double>();  // Store RHS with key -1
                    stat_constr.push_back(constraint_map);
                }
                
                for (const auto& [key, value] : variables_q.items()) {
                    variables[std::stoi(key)] = value.get<std::string>();
                }
                
                // Run solver for each run
                std::cout << "Running query " << i << std::endl;
                for (int j = 0; j < n_runs; j++) {
                    std::chrono::steady_clock::time_point begin_total = std::chrono::steady_clock::now();
                    std::vector<double> results_vector = naive_build_and_solve(
                        submod_monoton_constraints,
                        stat_constr,
                        variables,
                        objective_function,
                        i,
                        0  // run_id = 0 for full queries
                    );
                    double results = results_vector[0];
                    double build_time = results_vector[1];
                    double solve_time = results_vector[2];
                    std::chrono::steady_clock::time_point end_total = std::chrono::steady_clock::now();
                    int total_time = std::chrono::duration_cast<std::chrono::nanoseconds>(end_total - begin_total).count();
                    
                    if (j != 0) {  // Skip first run for warm-up
                        out << i << "," << j << "," << build_time << "," << solve_time << "," << results << std::endl;
                    }
                }
            }
        }
    }

    return 0;
}