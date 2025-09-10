
def create_variable_mappings(statistics: dict, join_graph: dict) -> list[dict]:
    """
    Create variable mappings for the given statistics and join graph.
    """
    
    dict_rel_join_vars = join_graph["join_pool_alias_map"]
    join_vars = list(dict_rel_join_vars.values())
    # flatten the lst
    join_vars = [item for sublist in join_vars for item in sublist]
    join_vars = list(set(join_vars))

    max_key = max(join_vars)
    
    rels = list(join_graph["join_pool_alias_map"].keys())
    dict_rel_idx = dict(zip(rels, range(-1, -len(rels) - 1, -1)))
    
    dict_rel_data_vars = {}
    dict_rel_data_vars_join_key = {}
    dict_rel_data_var_join_var_combi = {}
    
    cur_data_var_key = max_key + 1
    
    for key, value in dict_rel_join_vars.items():
        dict_rel_data_var_join_var_combi[key] = {}
        rel = key
        data_vars = []
        data_vars_join_key = {}
        dict_rel_data_vars_join_key[rel] = {}
        for join_key in value:
            data_vars.append(cur_data_var_key)
            dict_rel_data_vars_join_key[rel][join_key] = cur_data_var_key
            dict_rel_data_var_join_var_combi[rel][join_key] = cur_data_var_key
            
            cur_data_var_key += 1
            
        dict_rel_data_vars[rel] = data_vars
    
    data_vars = list(dict_rel_data_vars.values())
    data_vars = [item for sublist in data_vars for item in sublist]
    data_vars = list(set(data_vars))
    
    merged_dict = {k: dict_rel_join_vars.get(k, []) + dict_rel_data_vars.get(k, []) for k in set(dict_rel_data_vars) | set(dict_rel_join_vars)}
    
    all_vars = list(set(join_vars + data_vars))
    
    join_pool_map = join_graph["join_pool_map"]
    dict_mapping_rel_idx = {}
    for key, value in join_pool_map.items():
        rel, id_name = key.split(":")
        idx = value
        dict_mapping_rel_idx[(rel, id_name)] = (rel, idx)
    
    dict_statistics = {}
    for key, value in statistics.items():
        rel, join_var = key
        join_var_idx = dict_mapping_rel_idx[(rel, join_var)][1]
        data_var = dict_rel_data_var_join_var_combi[rel][join_var_idx]
        dict_statistics[(join_var_idx, data_var)] = value
        
    
    
    return  {
        "all_vars": all_vars,
        "dict_rel_join_vars": dict_rel_join_vars,
        "dict_rel_data_vars": dict_rel_data_vars,
        "dict_rel_vars": merged_dict,
        "dict_statistics": dict_statistics,
        
    }
    