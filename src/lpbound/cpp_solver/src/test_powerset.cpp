#include <iostream>
#include <vector>
#include <bitset>
#include <iomanip>

// Include the full function definition directly
// (alternatively to using a separate header)
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

void printPowerset(const std::vector<std::vector<int>>& powerset, const std::vector<int>& original) {
    std::cout << "Original set: { ";
    for (size_t i = 0; i < original.size(); ++i) {
        std::cout << original[i];
        if (i < original.size() - 1) std::cout << ", ";
    }
    std::cout << " }\n\n";

    std::cout << "Power set (size " << powerset.size() << "):\n";
    for (size_t i = 0; i < powerset.size(); ++i) {
        std::cout << std::setw(4) << i << ": { ";
        for (size_t j = 0; j < powerset[i].size(); ++j) {
            std::cout << powerset[i][j];
            if (j < powerset[i].size() - 1) std::cout << ", ";
        }
        std::cout << " }\n";
    }
}

int main() {
    std::cout << "Testing generatePowerset function\n";
    std::cout << "================================\n\n";

    // Test case 1: Empty set
    std::vector<int> emptySet;
    auto powerset1 = generatePowerset(emptySet);
    std::cout << "Test 1: Empty set\n";
    printPowerset(powerset1, emptySet);
    std::cout << "\n";

    // Test case 2: Single element
    std::vector<int> singleElement = {42};
    auto powerset2 = generatePowerset(singleElement);
    std::cout << "Test 2: Single element\n";
    printPowerset(powerset2, singleElement);
    std::cout << "\n";

    // Test case 3: Small set
    std::vector<int> smallSet = {1, 2, 3};
    auto powerset3 = generatePowerset(smallSet);
    std::cout << "Test 3: Small set\n";
    printPowerset(powerset3, smallSet);
    std::cout << "\n";

    // Test case 4: Medium set
    std::vector<int> mediumSet = {10, 20, 30, 40};
    auto powerset4 = generatePowerset(mediumSet);
    std::cout << "Test 4: Medium set\n";
    printPowerset(powerset4, mediumSet);
    std::cout << "\n";

    // Test case 5: Verify powerset size (2^n)
    for (int n = 0; n <= 10; ++n) {
        std::vector<int> testSet(n);
        for (int i = 0; i < n; ++i) {
            testSet[i] = i + 1;
        }
        auto powerset = generatePowerset(testSet);
        std::cout << "Set size " << n << " -> powerset size: " << powerset.size() 
                  << " (2^" << n << " = " << (1 << n) << ")\n";
    }

    return 0;
} 