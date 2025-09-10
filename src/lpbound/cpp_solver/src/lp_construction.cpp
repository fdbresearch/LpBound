#include "lp_construction.h"
#include "Highs.h"
#include <vector>
#include <map>
#include <string>
#include <iostream>
#include <vector>
#include <bitset>
#include <set>
#include <algorithm>
#include <fstream>

std::vector<std::vector<int>> generatePowerset(const std::vector<int>& nums) {
    int n = nums.size();
    int powSize = 1 << n; // 2^n
    std::vector<std::vector<int>> powerset;
    powerset.reserve(powSize - 1); // Reserve space for all subsets except empty set
    
    // Start from 1 to skip the empty set (when i=0)
    for (int i = 1; i < powSize; i++) {
        std::vector<int> subset;
        subset.reserve(n); // Pre-allocate maximum possible size
        
        for (int j = 0; j < n; j++) {
            if ((i & (1 << j)) != 0) {
                subset.push_back(nums[j]);
            }
        }
        
        powerset.push_back(std::move(subset));
    }
    
    return powerset;
}

double elemental_shannon_lp(
    const std::vector<int>& all_vars,
    const std::map<std::pair<int, int>, std::vector<double>>& statistics
) {
    /* 
    all_vars: all join and data variables
    statistics: (Relation Name, Join Variables on which conditioned) -> statistics
    */
    
    std::vector<std::vector<int>> powerset = generatePowerset(all_vars);

    int num_vars = powerset.size();

    std::map<std::set<int>, int> set_to_index;
    for (int i = 0; i < num_vars; i++) {
        // Convert vector to set before using as key
        std::set<int> set_key(powerset[i].begin(), powerset[i].end());
        set_to_index[set_key] = i;
    }

    // get id of largest set
    std::set<int> largest_set(powerset[num_vars - 1].begin(), powerset[num_vars - 1].end());
    int max_set_id = set_to_index[largest_set];

    // create LP
    HighsModel model;
    HighsLp& lp = model.lp_;
    lp.a_matrix_.format_ = MatrixFormat::kRowwise;

    lp.num_col_ = num_vars;

    lp.offset_ = 0.0;
    lp.col_cost_.assign(num_vars, 0.0); 
    // we want to maximize the entropy of union of all variables
    lp.col_cost_[max_set_id] = 1.0;  

    lp.sense_ = ObjSense::kMaximize;

    lp.col_lower_.assign(num_vars, 0.0);
    lp.col_upper_.assign(num_vars, 1e30);


    std::vector<int> start;
    start.push_back(0);
    std::vector<int> index;
    std::vector<double> values;
        
    int total_nonzeros = 0;

    int submod_counter = 0;

    // create elemental submodularity inequalities

    // iterate over all subsets in powerset
    for (int i = 0; i < num_vars; i++) {
        // get i-th set
        std::set<int> set_Z(powerset[i].begin(), powerset[i].end());
        // iterate over all variables in all_vars
        for (int j = 0; j < (int)all_vars.size(); j++) {
            for (int k = j+1; k < (int)all_vars.size(); k++) {

                // if x not in set_Z and y not in set_Z and x != y
                if (set_Z.find(all_vars[j]) == set_Z.end() && set_Z.find(all_vars[k]) == set_Z.end() && all_vars[j] != all_vars[k]) {

                    std::set<int> set_x = {all_vars[j]};
                    std::set<int> set_y = {all_vars[k]};

                    std::set<int> set_Z_cup_x = set_Z;
                    set_Z_cup_x.insert(all_vars[j]);

                    std::set<int> set_Z_cup_y = set_Z;
                    set_Z_cup_y.insert(all_vars[k]);

                    std::set<int> set_Z_cup_x_cup_y = set_Z_cup_x;
                    set_Z_cup_x_cup_y.insert(all_vars[k]);

                    int idx_Z = set_to_index[set_Z];
                    int idx_Z_cup_x = set_to_index[set_Z_cup_x];
                    int idx_Z_cup_y = set_to_index[set_Z_cup_y];
                    int idx_Z_cup_x_cup_y = set_to_index[set_Z_cup_x_cup_y];

                    index.push_back(idx_Z);
                    index.push_back(idx_Z_cup_x);
                    index.push_back(idx_Z_cup_y);
                    index.push_back(idx_Z_cup_x_cup_y);

                    // set signs
                    values.push_back(-1.0);
                    values.push_back(1.0);
                    values.push_back(1.0);
                    values.push_back(-1.0);

                    total_nonzeros += 4;
                    start.push_back(total_nonzeros);
                    submod_counter++;
                    
                }
            }
        }
    }

/*     
    for (int i = 0; i < num_vars; i++) {
        for (int j = 0; j < num_vars; j++) {
            if (i == j) {
                continue;
            }

            if (powerset[i].size() > powerset[j].size()) {
                continue;
            }
            // get i-th and j-th sets
            std::set<int> set_u(powerset[i].begin(), powerset[i].end());
            std::set<int> set_v(powerset[j].begin(), powerset[j].end());

            // if set_u is a subset of set_v, or if set_v is a subset of set_u, then skip. Else, get var ids
            if (std::includes(set_v.begin(), set_v.end(), set_u.begin(), set_u.end()) || std::includes(set_u.begin(), set_u.end(), set_v.begin(), set_v.end())) {
                continue;
            }

            // Compute the union and intersection
            std::set<int> set_u_cup_v;
            std::set<int> set_u_cap_v;
            
            // Union: insert all elements from both sets
            set_u_cup_v.insert(set_u.begin(), set_u.end());
            set_u_cup_v.insert(set_v.begin(), set_v.end());
            
            // Intersection: find common elements
            std::set_intersection(
                set_u.begin(), set_u.end(),
                set_v.begin(), set_v.end(),
                std::inserter(set_u_cap_v, set_u_cap_v.begin())
            );

            // get var ids
            int var_u = set_to_index[set_u];
            int var_v = set_to_index[set_v];
            int var_u_cup_v = set_to_index[set_u_cup_v];
            int var_u_cap_v = set_to_index[set_u_cap_v];

            index.push_back(var_u);
            index.push_back(var_v);
            index.push_back(var_u_cup_v);
            index.push_back(var_u_cap_v);

            // set signs
            values.push_back(-1.0);
            values.push_back(-1.0);
            values.push_back(1.0);
            values.push_back(1.0);

            total_nonzeros += 4;
            start.push_back(total_nonzeros);
            submod_counter++;
        }
    } */

    // create elemental monotonicity inequalities
    std::set<int> all_vars_set(powerset[num_vars - 1].begin(), powerset[num_vars - 1].end());
    int idx_all_vars = set_to_index[all_vars_set];
    // print ID of all_vars_set
    // std::cout << "ID of all_vars_set: " << idx_all_vars << std::endl;
    
    int monotonicity_counter = 0;
    for (int i = 0; i < (int)all_vars.size(); i++) {
        
        std::set<int> set_x = {all_vars[i]};
        int idx_x = set_to_index[set_x];
        // set all_vars_set  minus set_x
        std::set<int> set_all_vars_minus_x = all_vars_set;
        set_all_vars_minus_x.erase(all_vars[i]);
        int idx_all_vars_minus_x = set_to_index[set_all_vars_minus_x];

        index.push_back(idx_all_vars);
        index.push_back(idx_all_vars_minus_x);
        values.push_back(1.0);
        values.push_back(-1.0);

        total_nonzeros += 2;
        start.push_back(total_nonzeros);
        monotonicity_counter++;
    }

    // add 0s to rhs
    std::vector<double> rhs = std::vector<double>(monotonicity_counter + submod_counter, 1e30);

    // print size of rhs
    // std::cout << "size of rhs: " << rhs.size() << std::endl;

    // Use modern C++ iteration without destructuring
    for (const auto& stat_entry : statistics) {
        const auto& key = stat_entry.first;
        const auto& value = stat_entry.second;
        
        std::set<int> set_join_var = {key.first};
        int idx_join_var = set_to_index[set_join_var];
        std::set<int> set_join_data_var = {key.first, key.second};
        int idx_join_data_var = set_to_index[set_join_data_var];

        double p = 1.0;
        int len_statistics = value.size();
        // iterate over all statistics
        for (int i = 0; i < (len_statistics - 1); i++) {
            index.push_back(idx_join_var);
            index.push_back(idx_join_data_var);
            values.push_back(-1.0*(p-1.0));
            values.push_back(p);
            // cast p to double
            rhs.push_back(static_cast<double>(value[i])*static_cast<double>(p));
            p += 1.0;
            total_nonzeros += 2;
            start.push_back(total_nonzeros);
        }
        // add infinity norm constraint
        index.push_back(idx_join_var);
        index.push_back(idx_join_data_var);
        values.push_back(-1.0);
        values.push_back(1.0);
        rhs.push_back(static_cast<double>(value[len_statistics - 1]));
        total_nonzeros += 2;
        start.push_back(total_nonzeros);
    }
    int num_rows = rhs.size();
    lp.num_row_ = num_rows;

    // print size of rhs
    // std::cout << "size of rhs: " << rhs.size() << std::endl;
    

    lp.row_lower_.assign(num_rows, 0.0);
    lp.row_upper_ = rhs;

    lp.a_matrix_.start_ = start;
    lp.a_matrix_.index_ = index;
    lp.a_matrix_.value_ = values;
    
    // print size of matrix
    // std::cout << "size of matrix: " << lp.num_row_ << " " << lp.num_col_ << std::endl;

    Highs highs;
    highs.setOptionValue("presolve", "off");
    highs.setOptionValue("output_flag", false);
    highs.setOptionValue("log_to_console", false);
    highs.setOptionValue("write_solution_to_file", false);
    highs.setOptionValue("message_level", 0);

    highs.passModel(lp);

    HighsStatus solveStatus = highs.run();  // Keeping the variable to avoid changing behavior

    // export lp
    // std::string filename = "model_check.mps";
    // HighsStatus writeStatus = highs.writeModel(filename);
    // if (writeStatus != HighsStatus::kOk) {
    //     std::cerr << "Failed to write the LP model to an MPS file." << std::endl;
    //     return - 1;
    // }

    const HighsInfo& info = highs.getInfo();
    return info.objective_function_value;
}