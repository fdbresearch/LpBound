#ifndef FLOW_BOUND_H
#define FLOW_BOUND_H

#include <set>
#include <vector>

using namespace std;



// Union of two sets
template <typename T>
set<T> set_union(const set<T>& X, const set<T>& Y) {
    set<T> Z;
    copy(X.begin(), X.end(), inserter(Z, Z.end()));
    copy(Y.begin(), Y.end(), inserter(Z, Z.end()));
    return Z;
}


// A representation of a constraint on an Lp-norm of a degree sequence. The constraint is:
// log_2 ||deg(Y|X)||_p <= b
template <typename T>
struct DC {
    // Specify the degree sequence deg(Y|X) using the sets X and Y
    const set<T> X;
    const set<T> Y;
    // Specify the Lp-norm to be used
    const double p;
    // Specify the upper bound on the Lp-norm of the degree sequence *ON LOG SCALE*
    // log_2 ||deg(Y|X)||_p <= b
    const double b;

    DC(const set<T>& X_, const set<T>& Y_, double p_, double b_)
    // Note that this constructor always sets Y to be `X_ union Y_`
    : X(X_), Y(set_union(X_, Y_)), p(p_), b(b_) {
        assert(p > 0.0);
        assert(b >= 0.0);
    }
};

// Declare the flow_bound function
std::vector<double> flow_bound(
    const std::vector<DC<std::string>>& dcs,
    const std::vector<std::string>& target_vars,
    bool use_only_chain_bound,
    bool use_weighted_edges
);

#endif // FLOW_BOUND_H