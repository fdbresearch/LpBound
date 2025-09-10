import pandas as pd
import json
import os
import itertools
from itertools import *
from ortools.linear_solver import pywraplp
import sys
from lpbound.config.paths import LpBoundPaths

benchmarks = {
    
}

def normalize_ent_var(var:str):
    if var.startswith("h_"):
        return var
    elif var.startswith("_"):
        return "h" + var
    else:
        raise TypeError("Illegal variable name")

def get_objective_function(path, file, dict_vars):
    with open(path + file) as f:
        lines = f.readlines()
    
    for line in lines:
        if line.strip().startswith("Obj:"):
            obj_function = line.split(":")[1].strip()
            break
        else:
            continue
        
    if '0MI_IDX' in obj_function:
        obj_function = obj_function.replace('0MI_IDX', '0MI.IDX')
    
    obj_function = obj_function.split(' ')
    
    dict_objective = {}
    for i in range(0, len(obj_function), 2):
        coeff = float(obj_function[i])
        var = dict_vars[obj_function[i + 1].strip()]
        dict_objective[var] = coeff
    return dict_objective

def get_all_variables(path, file):

    query_vars = []
    dict_vars = {}
    start = 0
    
    counter_vars = 0
    
    with open(path + file) as f:
        lines = f.readlines()
        
    for line in lines:
        line = line.replace('0MI_IDX', '0MI.IDX')
        if line.startswith('Bounds'):
            start = 1
            continue
        if line.startswith('End'):
            break
        
        # replace multiple spaces in succession with a single space
        if start:
            var = line.split('<=')[1]
            var = var.strip()
            var_elem = var.split('_')
            # remove h
            var_elem = [v for v in var_elem if v != 'h']
            # sort the variable elements
            var_elem = sorted(var_elem)
            var = "h_" + "_".join(var_elem)
            dict_vars[str(counter_vars)] = var
            
            q_vars_split = var.split('_')
            # remove h
            q_vars_split = [q for q in q_vars_split if q != 'h']
            query_vars += q_vars_split
            
            counter_vars += 1
            
    query_vars = list(set(query_vars))
    return query_vars, dict_vars
        
def get_vars_from_stats(path, file):
    
    lines = []
    all_vars = []
    with open(path + file) as f:
        lines = f.readlines()
        
    for line in lines:
        l = line.replace('0MI_IDX', '0MI.IDX')
        l = l.strip()

        if l.startswith('Statistics_Ineq') or l.startswith('Dot_Product_Ineq'):
            vars = l.split(":")[1].split("<=")[0].strip()
            vars = vars.split(" ")
            vars = [v for v in vars if v.find("_") != -1]
            vars_clean = []
            for v in vars:
                if v.startswith("_"):
                    vars_clean.append("h" + v)
                else:
                    vars_clean.append(v)
            all_vars.extend(vars_clean)
    
    all_vars = list(set(all_vars))
    return all_vars

def get_stats_ineq(path, file, dict_vars):
    
    stats_ineq = {}
    start = 0
    counter_ineq = 0
    
    with open(path + file) as f:
        lines = f.readlines()
        
    counter_inner_prod_ineq = 0
    for line in lines:
        line = line.replace('0MI_IDX', '0MI.IDX')
        if line.startswith('Subject to'):
            start = 1
            continue
        
        if start and line.strip().startswith('Statistics_Ineq'):
            name = line.split('__')[0].strip()
            
            # count if "_|_" appears twice in line
            if line.count("_|_") == 2:
                p = line.split("_|_")[1].strip()
            else:
                p = line.split('_order_')[1].split(',')[0].strip()
            p = float(p)
            
            ineq = line.split(':')[1]
            
            rhs = ineq.split("<=")[1]
            rhs = rhs.strip()
            rhs = float(rhs)
            
            lhs = ineq.split("<=")[0]
            lhs = lhs.strip()
            
            if p == 1.0:
                lhs = lhs.split(' ')
                factor_ = lhs[0].strip()
                factor_ = float(factor_)
                
                variable_ = lhs[1].strip()
                variable_ = normalize_ent_var(variable_)
                var_id = dict_vars[variable_]
                stats_ineq[name] = {"lower_greater": "L", "variables": {var_id: factor_}, "rhs": rhs, "id": counter_ineq}
                                    
            elif p == float('inf'):
                # check if p is infinity
                factor_1 = 1.0
                factor_2 = -1.0
                
                variable_1 = normalize_ent_var(lhs.split(' ')[1].strip())
                variable_2 = normalize_ent_var(lhs.split(' ')[3].strip())
                
                if variable_2.replace("h_","").find(variable_1.replace('h_', '')) != -1:
                    temp = variable_1
                    variable_1 = variable_2
                    variable_2 = temp
                                
                stats_ineq[name] = {"lower_greater": "L", "variables": {dict_vars[variable_1]: factor_1, dict_vars[variable_2]: factor_2}, "rhs": rhs, "id": counter_ineq}
            else:
                factor_1 = p 
                factor_2 = (p - 1.0) * -1.0

                variable_1 = normalize_ent_var(lhs.split(' ')[1].strip())
                variable_2 = normalize_ent_var(lhs.split(' ')[3].strip())
                
                if variable_2.replace("h_","").find(variable_1.replace('h_', '')) != -1:
                    temp = variable_1
                    variable_1 = variable_2
                    variable_2 = temp
                
                stats_ineq[name] = {"lower_greater": "L", "variables": {dict_vars[variable_1]: factor_1, dict_vars[variable_2]: factor_2}, "rhs": rhs, "id": counter_ineq}
        
        elif start and line.strip().startswith('Dot_Product_Ineq'):
            lhs = line.split(':')[1].strip().split("<=")[0].strip()
            rhs = line.split('<=')[1].strip()
            rhs = float(rhs)
            
            name = "Dot_Product_Ineq._" + str(counter_inner_prod_ineq)
            
            lhs_els = lhs.split(' ')
            len_ = int(len(lhs_els) / 2)
            dict_variables_inner = {}
            for i in range(0, len_):
                dict_variables_inner[dict_vars["h" + lhs_els[i*2+1].strip()]] = float(lhs_els[i*2].strip())
            
            stats_ineq[name] = {"lower_greater": "L", "variables": dict_variables_inner, "rhs": rhs, "id": counter_ineq}
            counter_inner_prod_ineq += 1
        counter_ineq += 1
        
    return stats_ineq
            
def get_relation_vars(path, file):
    
    lines = []
    relation_vars = []
    with open(path + file) as f:
        lines = f.readlines()
        
    for line in lines:
        l = line.replace('0MI_IDX', '0MI.IDX')
        l = l.strip()

        if l.startswith('Statistics_Ineq') or l.startswith('Dot_Product_Ineq'):
            vars = l.split(":")[1].split("<=")[0].strip()
            vars = vars.split(" ")
            vars = [v for v in vars if v.startswith("h_") or v.startswith("_")]
            vars_clean = []
            for v in vars:
                if v.strip().startswith("_"):
                    vars_clean.append("h" + v.strip())
                else:
                    vars_clean.append(v)
            # get the largest variable
            vars_clean = sorted(vars_clean, key=lambda x: len(x), reverse=True)
            relation_vars.append(vars_clean[0])
            
    
    relation_vars = [v for v in relation_vars if len(v) > 0]
    relation_vars = list(set(relation_vars))
    
    return relation_vars

def variable_to_set(var):
    
    var = var.replace("h_", "")
    var = var.split('_')
    return set(var)

def V_less_x(V_set, x_set):
    
    V_less_x = V_set - x_set
    
    return V_less_x

def get_variable_dict(path, file):
    
    all_vars = get_vars_from_stats(path, file)
    # sort the variables
    all_vars = sorted(all_vars)
    
    dict_vars = {}
    counter_vars = 0
    for var in all_vars:
        dict_vars[var] = counter_vars
        counter_vars += 1
    
    return dict_vars

def get_elemental_monotonicities(relation_vars, dict_vars):
    
    dict_monotonicities = {}
    counter = 0
    for rel_var in relation_vars:
        rel_var_ = rel_var.replace("h_", "")
        rel_var_ = rel_var_.split('_')
        rel_var_set = set(rel_var_)
        
        for var in rel_var_set:
            V_less_x_set = V_less_x(rel_var_set, set([var]))
            V_less_x_list = list(V_less_x_set)
            V_less_x_list = sorted(V_less_x_list)
            V_less_x_var = "h_" + "_".join(V_less_x_list)
            
            # if V_less_x_var is not in the dictionary, add it
            if V_less_x_var not in dict_vars:
                dict_vars[V_less_x_var] = len(dict_vars)
                
            variables = {dict_vars[rel_var]: +1, dict_vars[V_less_x_var]: -1} 
            name_ = f'Monotonicity_Ineq._{counter}'
            
            dict_monotonicities[name_] = {'lower_greater': 'G', 'variables': variables, 'rhs': 0.0, 'id': counter}
            
            counter += 1
            
    return dict_vars, dict_monotonicities

def get_additivities(relation_vars, dict_vars):
    
    dict_additivities = {}
    counter = 0
    for rel_var in relation_vars:
        rel_var_ = rel_var.replace("h_", "")
        rel_var_ = rel_var_.split('_')
        rel_var_set = set(rel_var_)
        
        variables = {}
        for var in rel_var_set:
            var_ = "h_" + var
            
            if var_ not in dict_vars:
                dict_vars[var_] = len(dict_vars)
            
            variables[dict_vars[var_]] = -1.0
        
        variables[dict_vars[rel_var]] = 1.0
        name = f'Additivity_Ineq._{counter}'
        dict_additivities[name] = {'lower_greater': 'L', 'variables': variables, 'rhs': 0.0, 'id': counter}
        
        counter += 1
    
    for rel_var in relation_vars:
        rel_var_ = rel_var.replace("h_", "")
        rel_var_ = rel_var_.split('_')
        rel_var_set = set(rel_var_)
        
        
        for var in rel_var_set:
            
            var_ = "h_" + var
            
            if var_ in rel_var:
                
                variables = {}
                variables[dict_vars[rel_var]] =  1.0
                variables[dict_vars[var_]] = -1.0
                name = f'Additivity_Ineq._{counter}'
                dict_additivities[name] = {'lower_greater': 'G', 'variables': variables, 'rhs': 0.0, 'id': counter}
        
        counter += 1
    
    return dict_vars, dict_additivities

def get_submod_optimized(relation_variables, dict_vars):
    
    dict_optimized = {}
    
    all_vars = list(dict_vars.keys())
    join_vars = [v for v in all_vars if len(v) == 3]
    
    all_query_vars = [v.replace("h_", "").split("_") for v in relation_variables]   
    all_query_vars += [v.replace("h_", "").split("_") for v in join_vars]
    # flatten the list
    all_query_vars = list(itertools.chain(*all_query_vars))
    all_query_vars = list(set(all_query_vars))
    # sort the list
    all_query_vars.sort()
    query_entropy = "h_" + "_".join(all_query_vars)
    
    dict_vars[query_entropy] = len(dict_vars)

    variables = {dict_vars[query_entropy]: -1.0}
    # add all relation variables with a coefficient of 1
    for v in relation_variables:
        variables[dict_vars[v]] = 1.0
    

    # add all join variables with a coefficient of -1
    query_join_vars = [v.replace("h_", "") for v in join_vars]
    dict_query_join_vars_count = {}
    for jv in query_join_vars:
        dict_query_join_vars_count[jv] = -1
        for rv in relation_variables:
            if "_" + jv in rv:
                dict_query_join_vars_count[jv] += 1
    
    for v in join_vars:
        variables[dict_vars[v]] = dict_query_join_vars_count[v.replace("h_", "")] * -1.0

    name_ = "Submod_optimized"
    dict_optimized[name_] = {'lower_greater': 'G', 'variables': variables, 'rhs': 0.0, 'id': 0}

    return dict_optimized

def formulate_berge_objective(path, file):
    rel_vars = get_relation_vars(path, file)
    dict_vars = get_variable_dict(path, file)

    all_vars = list(dict_vars.keys())
    all_ent_vars_by_rel = [v.replace("h_", "").split("_") for v in rel_vars]
    all_ent_vars = [item for sublist in all_ent_vars_by_rel for item in sublist]
    all_ent_vars = list(set(all_ent_vars))
    
    dict_objective = {}
    for rel_var in rel_vars:
        dict_objective[dict_vars[rel_var]] = 1

    for v in all_ent_vars:
        counter = -1
        for el in all_ent_vars_by_rel:
            if v in el:
                counter += 1
        if counter > 0:
            dict_objective[dict_vars["h_" + v]] = -counter
    
    return dict_objective

def lpbound_base_allconstraints(path, file):
    
    path_output = f"{LpBoundPaths.PROJ_ROOT_DIR}/src/lpbound/cpp_solver/lpbound_parallel/input_data_full_queries/jobjoin_lpbase"

    query_id = file.split("_")[-1].split(".")[0]
    
    rel_vars = get_relation_vars(path, file)
    dict_vars = get_variable_dict(path, file)
    dict_vars, dict_monotonicities = get_elemental_monotonicities(rel_vars, dict_vars)
    
    dict_vars_inv = {v: k for k, v in dict_vars.items()}
    
    dict_stats_ineq = get_stats_ineq(path, file, dict_vars)
    all_ent_vars = list(dict_vars_inv.values())
    all_query_vars = [v.replace("h_", "").split("_") for v in all_ent_vars]
    all_query_vars = [v for vs in all_query_vars for v in vs]
    all_query_vars = list(set(all_query_vars))
    
    V = sorted(all_query_vars)

    super_set_vars = []
    super_set_entropies = []

    for r in range(1, len(V) + 1):
        for subset in itertools.combinations(V, r):
            super_set_vars.append(subset)
            entropy_ = "h_" + "_".join(sorted(subset))
            super_set_entropies.append(entropy_)

    dict_vars = {}
    for i, var in enumerate(super_set_vars):
        dict_vars[str(i)] = "h_" + "_".join(sorted(var))

    dict_vars_inv = {v: k for k, v in dict_vars.items()}

    dict_elemental_submod = {}
    dict_elemental_monotonicity = {}

    V_h = "h_" + "_".join(sorted(V))

    counter_monotonicity = 0
    for x in super_set_vars:
        
        if set(x) == set(V):
            continue
        
        V_less_x = set(V) - set(x)
        V_less_x = sorted(list(V_less_x))
        
        V_less_x_h = "h_" + "_".join(V_less_x)
        
        variables = {dict_vars_inv[V_h]: +1, dict_vars_inv[V_less_x_h]: -1}
        name_ = f'Monotonicity_Ineq._{counter_monotonicity}'
        dict_elemental_monotonicity[name_] = {"lower_greater": 'G', "variables": variables, 'rhs': 0.0, "id": counter_monotonicity}
        counter_monotonicity += 1
        

    counter_submod = 0
    for Z in super_set_vars:
        Z_list = list(Z)
        for x in all_query_vars:
            for y in all_query_vars:
                if (not x in Z_list) and (not y in Z_list) and (x != y):
                    
                    Z_cup_x = set(Z_list) | {x}
                    Z_cup_y = set(Z_list) | {y}
                    Z_cup_xy = set(Z_list) | {x, y}
                    

                    h_Z = "h_" + "_".join(sorted(Z))
                    h_Z_cup_x = "h_" + "_".join(sorted(Z_cup_x))
                    h_Z_cup_y = "h_" + "_".join(sorted(Z_cup_y))
                    h_Z_cup_xy = "h_" + "_".join(sorted(Z_cup_xy))
                    
                    constraint_name = f'Submodularity._{counter_submod}'
                    variables = {dict_vars_inv[h_Z_cup_x]: +1, 
                                dict_vars_inv[h_Z_cup_y]: +1, 
                                dict_vars_inv[h_Z_cup_xy]: -1, 
                                dict_vars_inv[h_Z]: -1}
                    
                    dict_elemental_submod[constraint_name] = {"lower_greater": 'G', "variables": variables, "rhs": 0.0, "id": counter_submod}
                    
                    counter_submod += 1
    
    stats_ineq = get_stats_ineq(path, file, dict_vars_inv)

    dict_elemental_monoto_and_submod = {**dict_elemental_monotonicity, **dict_elemental_submod}

    dict_objective = {}
    dict_vars_inv = {v: k for k, v in dict_vars.items()}
    dict_objective[dict_vars_inv[sorted(list(dict_vars.values()), key=lambda x: len(x), reverse=True)[0]]] = 1


    #return dict_elemental_monoto_and_submod, stats_ineq, dict_objective

    with open(f'{path_output}/shannon_optimized_{query_id}.json', 'w') as f:
            json.dump(dict_elemental_monoto_and_submod, f)
            
            
    with open(f'{path_output}/query_vars_{query_id}.json', 'w') as f:
        json.dump(dict_vars, f)
        
    with open(f'{path_output}/query_stats_ineq_{query_id}.json', 'w') as f:
        json.dump(stats_ineq, f)
        
    with open(f'{path_output}/objective_function_{query_id}.json', 'w') as f:
        json.dump(dict_objective, f)



def lbound_berge_all_constraints(path, file):

    rel_vars = get_relation_vars(path, file)
    
    dict_vars = get_variable_dict(path, file)
    dict_vars, dict_monotonicities = get_elemental_monotonicities(rel_vars, dict_vars)
    dict_vars, dict_additivities = get_additivities(rel_vars, dict_vars)
    dict_stats_ineq = get_stats_ineq(path, file, dict_vars)
    dict_optimized = get_submod_optimized(rel_vars, dict_vars)

    # merge dict_monotonicities, dict_additivities, dict_stats_ineq, dict_optimized
    dict_monotonicities_additivities_optim = {**dict_monotonicities, **dict_additivities, **dict_optimized}
    dict_objective  = formulate_berge_objective(path, file)
    return dict_monotonicities_additivities_optim, dict_stats_ineq, dict_vars, dict_objective
