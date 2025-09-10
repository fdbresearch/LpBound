import importlib
import sys
import os

# Proper path handling for imports
current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from . import utility as u

import sqlparse
from sqlparse.sql import Identifier, Comparison, Where, IdentifierList
from sqlparse.tokens import Keyword, DML

class UnionFind:
    def __init__(self):
        self.parent = {}
        
    def find(self, item):
        if item not in self.parent:
            self.parent[item] = item
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]
    
    def union(self, item1, item2):
        root1 = self.find(item1)
        root2 = self.find(item2)
        if root1 != root2:
            self.parent[root2] = root1

def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                yield from extract_from_part(item)
            elif item.ttype is Keyword:
                return
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def extract_where_part(parsed):
    where_seen = False
    for item in parsed.tokens:
        if where_seen:
            if is_subselect(item):
                yield from extract_where_part(item)
            elif item.ttype is Keyword and item.value.upper() in ['GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET']:
                return
            else:
                yield item
        elif isinstance(item, Where):
            where_seen = True
            yield from item.tokens

def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier
        elif isinstance(item, Identifier):
            yield item

        elif item.ttype is Keyword:
            yield item

def extract_tables(sql):
    stream = extract_from_part(sqlparse.parse(sql)[0])
    return list(extract_table_identifiers(stream))


def extract_conditions(sql):
    parsed = sqlparse.parse(sql)[0]
    stream = extract_where_part(parsed)
    conditions = []
    for item in stream:
        if isinstance(item, Comparison):
            conditions.append(item.value)
    return conditions

def is_join_condition(left, right):
    return '.' in left and '.' in right

def extract_join_conditions(sql):
    parsed = sqlparse.parse(sql)[0]
    stream = extract_where_part(parsed)
    join_conditions = []
    for item in stream:
        if isinstance(item, Comparison):
            join_conditions.append(item.value)
    return join_conditions

def find_join_variables(conditions):
    uf = UnionFind()
    
    for condition in conditions:
        left, operator, right = condition.partition('=')
        left = left.strip()
        right = right.strip()
        
        if is_join_condition(left, right):
            uf.union(left, right)
    
    groups = {}
    for var in uf.parent.keys():
        root = uf.find(var)
        if root not in groups:
            groups[root] = set()
        groups[root].add(var)
    
    return list(groups.values())

def name_join_variables(unique_join_variables):
    join_variable_names = {}
    for i, var_set in enumerate(unique_join_variables):
        join_variable_names[chr(65 + i)] = var_set
    return join_variable_names

def extract_and_name_join_variables(sql):
    conditions = extract_conditions(sql)
    unique_join_variables = find_join_variables(conditions)
    named_join_variables = name_join_variables(unique_join_variables)
    return named_join_variables


def extract_predicates_and_joins(sql):
    parsed = sqlparse.parse(sql)[0]
    stream = extract_where_part(parsed)
    predicates = []
    joins = []
    for item in stream:
        if isinstance(item, Comparison):
            comparison = item.value
            left, operator, right = comparison.partition('=')
            left = left.strip()
            right = right.strip()
            if '.' in left and '.' in right and left.split('.')[0] != right.split('.')[0]:
                joins.append(comparison)
            else:
                predicates.append(comparison)
    return predicates, joins


def extract_predicates(sql):
    predicates, _ = extract_predicates_and_joins(sql)
    return predicates


def extract_joins(sql):
    _, joins = extract_predicates_and_joins(sql)
    return joins


def extract_table_aliases(sql):
    stream = extract_from_part(sqlparse.parse(sql)[0])
    table_alias_mapping = {}
    for identifier in extract_table_identifiers(stream):
        if isinstance(identifier, Identifier):
            alias = identifier.get_alias() or identifier.get_real_name()
            name = identifier.get_real_name()
            if name not in table_alias_mapping:
                table_alias_mapping[name] = []
            if alias:
                table_alias_mapping[name].append(alias)
    return table_alias_mapping

def extract_data_variables(sql):
    conditions = extract_conditions(sql)
    data_variables = set()
    
    for condition in conditions:
        left, operator, right = condition.partition('=')
        left = left.strip()
        right = right.strip()
        
        if not is_join_condition(left, right):
            if '.' in left:
                data_variables.add(left)
            if '.' in right:
                data_variables.add(right)
    
    return data_variables


def get_all_relations(sql):
    tables = extract_tables(sql)
    relations = {}
    for table in tables:
        relations[table.get_alias()] = table.get_real_name()
        
    return relations


def build_predicate_relation_map(sql):
    """ 
    Returns a map with relation alias as key and the corresponding constraint as value
    """
    
    predicates, _ = extract_predicates_and_joins(sql)
    
    relations = get_all_relations(sql)
    aliases = list(relations.keys())
    
    constraint_relation_map = {}
    for alias in aliases:
        constraint_relation_map[alias] = []
        
    # Note: for now, onl equality constraints are considered! 
    for predicate in predicates:
        left, operator, right = predicate.partition('=')
        # if right is a number, then switch left and right
        if right.isnumeric():
            left, right = right, left
            
        left = left.strip()
        right = right.strip()
        
        left_alias = left.split('.')[0]
        left_var = left.split('.')[1]
        constraint_relation_map[left_alias].append((left_var, right))
        
    return constraint_relation_map
        
def build_join_relation_map(sql):
    joins = extract_joins(sql)
    
    join_map_left = {}
    join_map_right = {}
    
    for join in joins:
        left, operator, right = join.partition('=')
        left = left.strip()
        right = right.strip()
        operator = operator.strip()
        
        if operator != '=':
            raise ValueError("Only equality joins are supported")
        
        if left in join_map_left:
            join_map_left[left].append(right)
        else:
            join_map_left[left] = [right]
            
        if right in join_map_right:
            join_map_right[right].append(left)
        else:
            join_map_right[right] = [left]
            
    return join_map_left, join_map_right
        
       