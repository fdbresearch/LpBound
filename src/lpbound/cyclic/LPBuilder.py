import sys
import os

# Proper path handling for imports
current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(parent_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from . import QueryParser as qp
from .StatsGenerator import StatsGenerator
from ortools.linear_solver import pywraplp
import numpy as np

from ..LpFlow.flow_bound_wrapper import py_flow_bound

from itertools import combinations

class LPBuilder:
    """ 
    Note: the SQL has to be cleaned first!
    i.e. if there are joins with the vertex relation but no predicates
    on vertex relation, then the join has to be removed from the SQL! This is 
    does not change the cardinality as the join is not constrained.
    We know that all edge sources and targets are contained in vertex relation
    """
    
    def __init__(self, 
                 dataset:str, 
                 sql:str,
                 statistics:dict):
        
        self.solver = pywraplp.Solver.CreateSolver("GLOP")
        self.infinity = self.solver.infinity()
        
        self.statistics = statistics
        
        self.dataset = dataset
        self.sql = sql
        
        self.lp_stats_constraints = {}
        self.lp_vars = {}
        
        self.counter_stats_ineq = 0
        
        self.join_vars = qp.extract_and_name_join_variables(self.sql)
        self.data_vars = qp.extract_data_variables(self.sql)
        self.data_vars = ["l" + var.split(".")[0].replace("v", "") for var in self.data_vars]
        
        self.relations = qp.get_all_relations(self.sql)
        self.predicate_relation_map = qp.build_predicate_relation_map(self.sql)
        
        for jv in self.join_vars.keys():
            self.add_variable_to_lp(f"h_{jv}")
        
        self.join_map_left, self.join_map_right = qp.build_join_relation_map(self.sql)

        self.inverted_named_join_variables = {}
        for key, value in self.join_vars.items():
            for var in value:
                self.inverted_named_join_variables[var] = key

        self.superset_entropies = []
        self.superset_vars = []
        
        self.build_status = False
        
        self.target_variabel = None
        
    def export_lp(self, path:str):
        with open(path, 'w') as file:
            file.write(self.solver.ExportModelAsLpFormat(False))
    
    def add_variable_to_lp(self, name:str, lb:float=0, ub:float=None):
        
        if name.find("l") != -1:
            return
        
        if name == "h":
            raise ValueError("Variable name cannot be 'h'")
        if ub is None:
            ub = self.infinity
            
        if name not in self.lp_vars:
            
            # print(f"Added {name} to LP")
            self.lp_vars[name] = self.solver.NumVar(lb, ub, name)
    
    def build_and_solve_flow_bound(self, max_p=30):
        
        # {{"1"}, {"1", "0MI_IDX"}, 5.0, log2(3.017088168272582)},
        vars = set()
        
        dcs = []
                
        relation_lp_var_map = {}
        # print("Adding statistics constraints")
        for relation_alias in self.relations.keys():
                        
            constraint_type, predicates = self.check_relation_constraints(relation_alias)
                            
            # process vertex relation
            if relation_alias.find("v") != -1:
                continue
                
            elif relation_alias.find("e") != -1:
                jv1 = f"{relation_alias}.s"
                jv2 = f"{relation_alias}.t"
                
                jv1_mapped = self.inverted_named_join_variables[jv1]
                jv2_mapped = self.inverted_named_join_variables[jv2]
                
                entropy_var_jv1 = f"h_{jv1_mapped}"
                entropy_var_jv2 = f"h_{jv2_mapped}"
                
                if jv1_mapped < jv2_mapped:
                    entropy_var_jv1_jv2 = f"h_{jv1_mapped}_{jv2_mapped}"
                else:
                    entropy_var_jv1_jv2 = f"h_{jv2_mapped}_{jv1_mapped}"
                                
                match constraint_type:
                    
                    case "double_constraint":
                        #print("Double constraint")
                        dcs_, vars_ = self.add_double_constraint_statistic_edge_flow_bound(relation_alias, predicates, entropy_var_jv1, entropy_var_jv2)
                        dcs += dcs_
                        vars.update(vars_)
                    case _:
                        raise ValueError("Constraint type not implemented")
                
                
            else:
                raise ValueError("Unknown relation type")
                
        objective_value = py_flow_bound(dcs, list(vars))
        
        return objective_value
    
    def build(self):
        
        #try:
        relation_lp_var_map = {}
        # print("Adding statistics constraints")
        for relation_alias in self.relations.keys():
                        
            constraint_type, predicates = self.check_relation_constraints(relation_alias)
                            
            # process vertex relation
            if relation_alias.find("v") != -1:
                continue
                id_var = f"{relation_alias}.i"
                label_var = f"{relation_alias}.l"        
                id_var_mapped = self.inverted_named_join_variables[id_var]
                label_var_named = f"l{relation_alias.replace('v', '')}"
                
                entropy_var_id = f"h_{id_var_mapped}"
                entropy_var_label = f"h_{label_var_named}"
                entropy_var_id_label = f"h_{label_var_named}_{id_var_mapped}"
                

                match constraint_type:
                    case "unconstrained":
                        self.add_unconstrained_statistic_vertex(relation_alias, entropy_var_id, entropy_var_label, entropy_var_id_label)
                    case "single_constraint":
                        self.add_single_constraint_statistic_vertex(relation_alias, predicates, entropy_var_id, entropy_var_label, entropy_var_id_label)
                    case _:
                        raise ValueError("Unknown constraint type")
                
            elif relation_alias.find("e") != -1:
                jv1 = f"{relation_alias}.s"
                jv2 = f"{relation_alias}.t"
                
                jv1_mapped = self.inverted_named_join_variables[jv1]
                jv2_mapped = self.inverted_named_join_variables[jv2]
                
                entropy_var_jv1 = f"h_{jv1_mapped}"
                entropy_var_jv2 = f"h_{jv2_mapped}"
                
                if jv1_mapped < jv2_mapped:
                    entropy_var_jv1_jv2 = f"h_{jv1_mapped}_{jv2_mapped}"
                else:
                    entropy_var_jv1_jv2 = f"h_{jv2_mapped}_{jv1_mapped}"
                
                self.add_variable_to_lp(entropy_var_jv1)
                self.add_variable_to_lp(entropy_var_jv2)
                self.add_variable_to_lp(entropy_var_jv1_jv2)
                
                match constraint_type:
                    case "unconstrained":
                        print("Unconstrained")
                        self.add_unconstrained_statistic_edge(relation_alias, entropy_var_jv1, entropy_var_jv2, entropy_var_jv1_jv2)
                    case "single_constraint":
                        print("Single constraint")
                        self.add_single_constrained_statistic_edge(relation_alias, predicates, entropy_var_jv1, entropy_var_jv2, entropy_var_jv1_jv2)
                    case "double_constraint":
                        #print("Double constraint")
                        self.add_double_constraint_statistic_edge(relation_alias, predicates, entropy_var_jv1, entropy_var_jv2, entropy_var_jv1_jv2)
                    case _:
                        raise ValueError("Unknown constraint type")
                
                
            else:
                raise ValueError("Unknown relation type")
        
        # generate all LP variables
        print("Generating LP variables")
        self.generate_superset()
        self.set_target_variable()
        
        print("Adding elemental submodularities")
        self.add_elemental_submodularities()
        # print(self.lp_vars)
        print("Adding elemental monotonicities")
        self.add_elemental_monotonicities()
        # print(self.lp_vars)
        
        # set objective
        # maximize list(self.lp_vars.keys())[-1]
        print("Setting objective")
        self.solver.Maximize(self.lp_vars[self.target_variabel])            
        
        self.build_status = True
        # print("LP successfully built")
        
        #except Exception as e:
        #    print(e)
        #    self.build_status = False
        #    print("Building LP failed")
            
    def solve(self):
        
        status = self.solver.Solve()
        
        if status == pywraplp.Solver.OPTIMAL:
            # print('Solution:')

            # print('Objective value =', self.solver.Objective().Value())
            return self.solver.Objective().Value()
        else:
            print('The problem does not have an optimal solution.')
    
    def add_unconstrained_statistic_vertex(
        self, 
        relation_alias:str,
        entropy_var_id:str,
        entropy_var_label:str,
        entropy_var_id_label:str
        ):
        print(relation_alias + " unconstrained")
        
        # get relation name
        relation_name = self.relations[relation_alias]
        
        ps = self.statistics[self.dataset]["vertex"][('i', 'unconstraint')].keys()
        for p in ps:
            norm = self.statistics[self.dataset]["vertex"][('i', 'unconstraint')][p]
            norm = np.log2(norm)
            h_i = self.lp_vars[entropy_var_id]
            h_i_l = self.lp_vars[entropy_var_id_label]
            
            ineq_name = f"stat_ineq_p.{str(p)}_{self.counter_stats_ineq}"
            
            if p == np.inf:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(h_i_l - h_i <= norm, ineq_name)
            else:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(p * h_i_l - (p - 1) * h_i <= p * norm, ineq_name)
            
            self.counter_stats_ineq += 1
        
    def add_single_constraint_statistic_vertex(
        self, 
        relation_alias:str, 
        predicates:tuple,
        entropy_var_id:str,
        entropy_var_label:str,
        entropy_var_id_label:str
        ):
        
        relation_name = self.relations[relation_alias]
        predicate_value = int(predicates[0][0][1])
        
        self.add_variable_to_lp(entropy_var_id)
        self.add_variable_to_lp(entropy_var_label)
        self.add_variable_to_lp(entropy_var_id_label)
        
        ps = self.statistics[self.dataset]["vertex"][('i', 'single_constraint')][predicate_value].keys()
                
        for p in ps:
            norm = self.statistics[self.dataset]["vertex"][('i', 'single_constraint')][predicate_value][p]
            norm = np.log2(norm)
            h_i = self.lp_vars[entropy_var_id]
            h_i_l = self.lp_vars[entropy_var_id_label]
            ineq_name = f"stat_ineq_p.{str(p)}_{self.counter_stats_ineq}"
            
            if p == np.inf:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(h_i_l - h_i <= norm, ineq_name)
            else:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(p * h_i_l - (p - 1) * h_i <= p * norm, ineq_name)
            
            self.counter_stats_ineq += 1
            
    def add_unconstrained_statistic_edge(
        self,
        relation_alias:str,
        entropy_var_jv1:str,
        entropy_var_jv2:str,
        entropy_var_jv1_jv2:str
        ):
        
        ps = self.statistics[self.dataset]["edge"][('s', 'unconstraint')].keys()
        
        for p in ps:
            norm = self.statistics[self.dataset]["edge"][('s', 'unconstraint')][p]
            norm = np.log2(norm)
            h_s = self.lp_vars[entropy_var_jv1]
            h_s_t = self.lp_vars[entropy_var_jv1_jv2]
            ineq_name = f"stat_ineq_p.{str(p)}_{self.counter_stats_ineq}"
            
            if p == np.inf:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(h_s_t - h_s <= norm, ineq_name)
            else:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(p * h_s_t - (p - 1) * h_s <= p * norm, ineq_name)
            
            self.counter_stats_ineq += 1
            
        ps = self.statistics[self.dataset]["edge"][('t', 'unconstraint')].keys()
        
        for p in ps:
            norm = self.statistics[self.dataset]["edge"][('t', 'unconstraint')][p]
            norm = np.log2(norm)
            h_t = self.lp_vars[entropy_var_jv2]
            h_s_t = self.lp_vars[entropy_var_jv1_jv2]
            ineq_name = f"stat_ineq_p.{str(p)}_{self.counter_stats_ineq}"
            
            if p == np.inf:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(h_s_t - h_t <= norm, ineq_name)
            else:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(p * h_s_t - (p - 1) * h_t <= p * norm, ineq_name)
            
            self.counter_stats_ineq += 1
        
        
    def add_single_constrained_statistic_edge(
        self,
        relation_alias:str,
        predicates:tuple,
        entropy_var_jv1:str,
        entropy_var_jv2:str,
        entropy_var_jv1_jv2:str
        ):
        
        # predicate = ("single_constraint", ("t", target_join_partner))
        # Note: only single constraints on source s are considered!!
        constraint_var = predicates[0]
        print(relation_alias)
        print(predicates)
        join_partner = predicates[1][0].split(".")[0]
        
        # find constraint on join partner
        constraint = self.predicate_relation_map[join_partner]
        data_val = int(constraint[0][1])
        
        if constraint_var == "s":
        # TODO: we can also add unconstrained statistics for source t!
        
            ps = self.statistics[self.dataset]["edge"][('s', 'single_constraint')][data_val].keys()
            
            for p in ps:
                norm = self.statistics[self.dataset]["edge"][('s', 'single_constraint')][data_val][p]
                h_s = self.lp_vars[entropy_var_jv1]
                h_s_t = self.lp_vars[entropy_var_jv1_jv2]
                ineq_name = f"stat_ineq_p.{str(p)}_{self.counter_stats_ineq}"
                
                if p == np.inf:
                    self.lp_stats_constraints[ineq_name] = self.solver.Add(h_s_t - h_s <= norm, ineq_name)
                else:
                    self.lp_stats_constraints[ineq_name] = self.solver.Add(p * h_s_t - (p - 1) * h_s <= p * norm, ineq_name)
                
                self.counter_stats_ineq += 1
            
        elif constraint_var == "t":
        # TODO: we can also add unconstrained statistics for source s!
            
            ps = self.statistics[self.dataset]["edge"][('t', 'single_constraint')][data_val].keys()
            
            for p in ps:
                norm = self.statistics[self.dataset]["edge"][('t', 'single_constraint')][data_val][p]
                norm = np.log2(norm)
                h_t = self.lp_vars[entropy_var_jv2]
                h_s_t = self.lp_vars[entropy_var_jv1_jv2]
                ineq_name = f"stat_ineq_p.{str(p)}_{self.counter_stats_ineq}"
                
                if p == np.inf:
                    self.lp_stats_constraints[ineq_name] = self.solver.Add(h_s_t - h_t <= norm, ineq_name)
                else:
                    self.lp_stats_constraints[ineq_name] = self.solver.Add(p * h_s_t - (p - 1) * h_t <= p * norm, ineq_name)
                
                self.counter_stats_ineq += 1
            
    def add_double_constraint_statistic_edge(
        self, 
        relation_alias:str, 
        predicates:tuple,
        entropy_var_jv1:str,
        entropy_var_jv2:str,
        entropy_var_jv1_jv2:str
        ):
       
        join_partner_s = predicates[0][0].split(".")[0]
        join_partner_t = predicates[1][0].split(".")[0]

        
        # find constraint on join partner
        constraint_s = self.predicate_relation_map[join_partner_s]
        constraint_t = self.predicate_relation_map[join_partner_t]
        data_val_s = np.int32(constraint_s[0][1])
        data_val_t = np.int32(constraint_t[0][1])       
            
        ps = self.statistics[self.dataset]["edge"][('s', 'double_constraint')][(data_val_s, data_val_t)].keys()
        
        for p in ps:
            norm = self.statistics[self.dataset]["edge"][('s', 'double_constraint')][(data_val_s, data_val_t)][p]
            norm = np.log2(norm)
            h_s = self.lp_vars[entropy_var_jv1]
            h_s_t = self.lp_vars[entropy_var_jv1_jv2]
            ineq_name = f"stat_ineq_p.{str(p)}_{self.counter_stats_ineq}"
            
            if p == np.inf:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(h_s_t - h_s <= norm, ineq_name)
            else:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(p * h_s_t - (p - 1) * h_s <= p * norm, ineq_name)
            
            self.counter_stats_ineq += 1
        
        ps = self.statistics[self.dataset]["edge"][('t', 'double_constraint')][(data_val_s, data_val_t)].keys()    
        
        for p in ps:
            norm = self.statistics[self.dataset]["edge"][('t', 'double_constraint')][(data_val_s, data_val_t)][p]
            norm = np.log2(norm)
            h_t = self.lp_vars[entropy_var_jv2]
            h_s_t = self.lp_vars[entropy_var_jv1_jv2]
            ineq_name = f"stat_ineq_p.{str(p)}_{self.counter_stats_ineq}"
            
            if p == np.inf:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(h_s_t - h_t <= norm, ineq_name)
            else:
                self.lp_stats_constraints[ineq_name] = self.solver.Add(p * h_s_t - (p - 1) * h_t <= p * norm, ineq_name)
            
            self.counter_stats_ineq += 1

    def check_relation_constraints(
        self, 
        relation_alias:str,
        )->str:
        
        # vertex relations are at most single constrained
        if relation_alias.find("v") != -1:
            # checl if there is a constraint in the predicate_relation_map
            try:
                predicates = self.predicate_relation_map[relation_alias]
            except:
                predicates = []
                
            if len(predicates) == 0:
                return ("unconstrained", (None, None))
            else:
                return ("single_constraint", (predicates, None))
                
        # edge relations can be double constrained
        elif relation_alias.find("e") != -1:
            # get aliases of vertex relation that join with source s or target t of edge relation
            
            # ------------------------------------------------------------------------------------
            # join partners of source s
            try:
                left_join_partner = self.join_map_left[relation_alias + ".s"]
            except:
                left_join_partner = []
                
            try:
                right_join_partner = self.join_map_right[relation_alias + ".s"]
            except:
                right_join_partner = []
                
            source_join_partner = left_join_partner + right_join_partner
            # remove duplicates
            source_join_partner = list(set(source_join_partner))
            source_join_partner = [x for x in source_join_partner if x.find("v") != -1]
            
            # ------------------------------------------------------------------------------------
            # join partners of target t
            try:
                left_join_partner = self.join_map_left[relation_alias + ".t"]
            except:
                left_join_partner = []
                
            try:
                right_join_partner = self.join_map_right[relation_alias + ".t"]
            except:
                right_join_partner = []
                
            target_join_partner = left_join_partner + right_join_partner
            # remove duplicates
            target_join_partner = list(set(target_join_partner))
            target_join_partner = [x for x in target_join_partner if x.find("v") != -1]
            
            if (len(source_join_partner) > 0) and (len(target_join_partner) > 0):
                return ("double_constraint", (source_join_partner, target_join_partner))
            
            elif (len(source_join_partner) == 0) and (len(target_join_partner) == 0):
                return ("unconstrained", (None, None))            

            elif (len(source_join_partner) > 0) or (len(target_join_partner) == 0):
                return ("single_constraint", ("s", source_join_partner))
            
            elif (len(source_join_partner) == 0) or (len(target_join_partner) > 0):
                return ("single_constraint", ("t", target_join_partner))
            

            else:
                raise ValueError("Unknown constraint type")          
            
        else:
            raise ValueError("Unknown relation type")
    
    def formulate_entropy(self, variables):
        label_vars = [var for var in variables if var.find("l") != -1]
        join_vars = [var for var in variables if var.find("l") == -1]
        
        label_vars.sort()
        join_vars.sort()
        
        entropy = "h_" + "_".join(label_vars) + "_" + "_".join(join_vars)
        entropy = entropy.replace("__", "_")
        
        # remove trailing "_"
        if entropy[-1] == "_":
            entropy = entropy[:-1]
        
        return entropy
    
    def add_elemental_submodularities(self):
        
        if len(self.lp_vars) == 0:
            raise ValueError("No variables in LP")
        
        lp_vars = [var for var in self.lp_vars.keys()]
        set_X = [var.split("_") for var in lp_vars]
        # latten list   
        set_X = [item for sublist in set_X for item in sublist]
        set_X = list(set(set_X))
        set_X.sort()
        # remove h
        set_X = [var for var in set_X if var != "h"]
        set_X = set_X + ["0"]
        
        # for any two variables x,y in set_X do the following:
        # add constraint of the form h(Z \cup {x}) + h(Z \cup {y}) - h(Z) - h(Z \cup {x,y}) >= 0, with Z = set_X \ {x,y}
        
        
    def generate_superset(self):
        """
        Generates the superset/powerset of the given variables
        
        Parameters:
        variables: list of variables
        
        Returns:
        superset: list of all possible combinations of the variables
        
        """
        print("Generating superset")
        print(self.lp_vars)
        if len(self.lp_vars) == 0:
            raise ValueError("No variables in LP")
        
        lp_vars = [var for var in self.lp_vars.keys()]
        set_X = [var.split("_") for var in lp_vars]
        print("Set X: " + str(set_X))
        # flatten list   
        set_X = [item for sublist in set_X for item in sublist]
        set_X = list(set(set_X))
        set_X.sort()
        # remove h
        set_X = [var for var in set_X if var != "h"]
        
        # add h_0 to LP
        #if "h_0" not in self.lp_vars:
        #    self.add_variable_to_lp("h_0", 0, 0)    
        #set_X = set_X + ["0"]
    
        superset_entropies = []
        superset_vars = []
        # generate combinations of the variables of all possible lengths
        for r in range(1, len(set_X) + 1):
            combs = combinations(set_X, r)
            for comb in combs:
                superset_entropies.append(self.formulate_entropy(comb))
                superset_vars.append(list(comb))
                
        self.superset_entropies = list(set(superset_entropies))
        self.superset_vars = superset_vars
        
        if len(self.superset_entropies) != len(self.superset_vars):
            raise ValueError("Length of named entropy variables and variables do not match")
            
    def set_target_variable(self):
        # the target variable is the entropy variable with the most elemental variables
        # find the largest set in the superset
        #target_var = self.superset_vars[0]
        #for var in self.superset_vars:
        #    if len(var) > len(target_var):
        #        target_var = var
                
        #self.target_variabel = self.formulate_entropy(target_var)
        
        # target value only includes join variables
        join_vars = [var for var in self.lp_vars.keys() if var.find("l") == -1]
        join_vars = [var.split("_") for var in join_vars]
        join_vars = [item for sublist in join_vars for item in sublist]
        # remove "h"
        join_vars = [var for var in join_vars if var != "h"]
        target_var = list(set(join_vars))
        target_var = self.formulate_entropy(target_var)
        
        self.target_variabel = target_var
        
    def  add_elemental_submodularities(self):
        
        superset = self.superset_vars
        
        # flatten superset
        all_elemental_vars = [item for sublist in superset for item in sublist]
        all_elemental_vars = list(set(all_elemental_vars))
        
        submod_counter = 1
        for Z in superset:
            
            for x in all_elemental_vars:
                for y in all_elemental_vars:
                    if (not x in Z) and (not y in Z) and (x != y):

                        name_h_Z_cup_x = self.formulate_entropy(Z + [x])
                        name_h_Z_cup_y = self.formulate_entropy(Z + [y])
                        name_h_Z = self.formulate_entropy(Z)
                        name_h_Z_cup_x_cup_y = self.formulate_entropy(Z + [x, y])
                        
                        # check if entropy vars already exist in lp_vars
                        # if not exist, add them to the LP
                        self.add_variable_to_lp(name_h_Z_cup_x)
                        self.add_variable_to_lp(name_h_Z_cup_y)
                        self.add_variable_to_lp(name_h_Z)
                        self.add_variable_to_lp(name_h_Z_cup_x_cup_y)
                            
                        # add constraint
                        # h_Z_cup_x + h_Z_cup_y - h_Z - h_Z_cup_x_cup_y >= 0
                        constrain_name = f"submod_{submod_counter}"
                        self.lp_stats_constraints[constrain_name] = self.solver.Add(
                            self.lp_vars[name_h_Z_cup_x] + self.lp_vars[name_h_Z_cup_y] - self.lp_vars[name_h_Z] - self.lp_vars[name_h_Z_cup_x_cup_y] >= 0, constrain_name) 
                        submod_counter += 1
                        
    def add_elemental_monotonicities(self):
        
        # V is the largest set in the superset
        # find largest set in superset
        V = self.superset_vars[0]
        for var in self.superset_vars:
            if len(var) > len(V):
                V = var
        #V = self.target_variabel.split("_")
        #V = self.data_vars + list(self.join_vars.keys())
        V = list(self.join_vars.keys())
        name_h_V = self.formulate_entropy(V)
        # print("V: " + name_h_V)
        
        monotonicity_counter = 1
        
        # check if name_h_V already exists in lp_vars
        if name_h_V not in self.lp_vars:
            self.lp_vars[name_h_V] = self.solver.NumVar(0, self.solver.infinity(), name_h_V)
        
        for x in self.superset_vars:
            if set(x) == set(V):
                continue
            
            # add constraints of the form: h(V) - h(V - {x}) >= 0
            # remove x from V
            V_minus_x = set(V) - set(x)
            
            name_h_V_minus_x = self.formulate_entropy(list(V_minus_x))

            # check if entropy vars already exist in lp_vars
            # if not exist, add them to the LP
            self.add_variable_to_lp(name_h_V_minus_x)
                
            constrain_name = f"monotonicity_{monotonicity_counter}"
            
            self.lp_stats_constraints[constrain_name] = self.solver.Add(
                self.lp_vars[name_h_V] - self.lp_vars[name_h_V_minus_x] >= 0, constrain_name)

            
            monotonicity_counter += 1

    def add_double_constraint_statistic_edge_flow_bound(
        self, 
        relation_alias:str, 
        predicates:tuple,
        entropy_var_jv1:str,
        entropy_var_jv2:str
        ):
        
        join_partner_s = predicates[0][0].split(".")[0]
        join_partner_t = predicates[1][0].split(".")[0]

        
        # find constraint on join partner
        constraint_s = self.predicate_relation_map[join_partner_s]
        constraint_t = self.predicate_relation_map[join_partner_t]
        data_val_s = np.int32(constraint_s[0][1])
        data_val_t = np.int32(constraint_t[0][1])  
        
        ps = self.statistics[self.dataset]["edge"][('s', 'double_constraint')][(data_val_s, data_val_t)].keys()
        
        dcs = []
        set_vars = set()
        
        for p in ps:
            norm = self.statistics[self.dataset]["edge"][('s', 'double_constraint')][(data_val_s, data_val_t)][p]
            norm = np.log2(norm)
            h_s = self.lp_vars[entropy_var_jv1]
            h_t = self.lp_vars[entropy_var_jv2]
            
            dc_1 = ((h_s), (h_s, h_t), p, norm)
            
            dcs.append(dc_1)
            set_vars.add(h_s)
            set_vars.add(h_t)
            
        for p in ps:
            norm = self.statistics[self.dataset]["edge"][('t', 'double_constraint')][(data_val_s, data_val_t)][p]
            norm = np.log2(norm)
            h_s = self.lp_vars[entropy_var_jv1]
            h_t = self.lp_vars[entropy_var_jv2]
            
            dc_1 = ((h_t), (h_t, h_s), p, norm)

            dcs.append(dc_1)
            set_vars.add(h_s)
            set_vars.add(h_t)
        
        return dcs, set_vars
