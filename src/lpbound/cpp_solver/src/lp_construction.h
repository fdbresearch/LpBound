#ifndef LP_CONSTRUCTION_H
#define LP_CONSTRUCTION_H

#include <vector>
#include <map>
#include <string>
#include <set>
#include <utility>

// Declare the generatePowerset function
std::vector<std::vector<int>> generatePowerset(const std::vector<int>& nums);

// Declare the elemental_shannon_lp function
double elemental_shannon_lp(
    const std::vector<int>& all_vars,
    const std::map<std::pair<int, int>, std::vector<double>>& statistics
);

#endif // LP_CONSTRUCTION_H 