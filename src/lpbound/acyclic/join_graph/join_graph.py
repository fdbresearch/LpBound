from collections import defaultdict

from lpbound.acyclic.join_graph.vertex import Vertex
from lpbound.acyclic.join_graph.edge import Edge
from lpbound.utils.types import AliasColPair


class UnionFind:
    """
    Union-Find data structure for finding connected components in a graph.
    This is used to create the transitive closure of the join condition.
    E.g., if we have a join condition "t.id = mi.movie_id AND t.id = mk.movie_id",
    we can use Union-Find to find the transitive closure of the join condition,
    i.e. add "t.id = mk.movie_id" to the join condition.
    """

    def __init__(self):
        # parent[x] = x's parent in the union-find tree
        self.parent: dict[AliasColPair, AliasColPair] = {}
        # rank[x] = rank (approximate tree height) for union by rank
        self.rank: dict[AliasColPair, int] = {}

    def find(self, x: AliasColPair) -> AliasColPair:
        """Path compression find."""
        if self.parent.get(x, x) != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent.get(x, x)

    def union(self, x: AliasColPair, y: AliasColPair) -> None:
        root_x = self.find(x)
        root_y = self.find(y)

        if root_x != root_y:
            rx_rank = self.rank.get(root_x, 0)
            ry_rank = self.rank.get(root_y, 0)
            # union by rank
            if rx_rank < ry_rank:
                self.parent[root_x] = root_y
            elif rx_rank > ry_rank:
                self.parent[root_y] = root_x
            else:
                self.parent[root_y] = root_x
                self.rank[root_x] = rx_rank + 1

    def make_set(self, x: AliasColPair) -> None:
        """Initialize a set for x if not present."""
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0


class JoinGraph:
    def __init__(self, is_groupby: bool = False):
        self.is_groupby: bool = is_groupby

        self.vertices: dict[str, Vertex] = {}  # alias -> Vertex
        self.edges: list[Edge] = []  # list of Edge

        self.uf: UnionFind = UnionFind()

        # We will build the "join pool" after we have all edges
        # (alias, col_name) -> pool_id
        self.join_pool_map: dict[AliasColPair, int] = {}
        # alias -> [pool_id]: map of aliases to the pool_ids they are in
        self.join_pool_alias_map: dict[str, list[int]] = {}
        # pool_id -> domain size/l0-norm
        self.join_pool_domain_size: dict[int, int] = {}

    def add_vertex(self, v: Vertex):
        self.vertices[v.alias] = v

    def add_edge(self, e: Edge):
        """
        Add an edge + unify (alias_left, join_var_left) with (alias_right, join_var_right).
        Also add those columns to the vertices' join_columns.
        """
        self.edges.append(e)

        # Ensure these columns appear in the union-find
        self.uf.make_set((e.alias_left, e.join_var_left))
        self.uf.make_set((e.alias_right, e.join_var_right))

        # Union them
        self.uf.union((e.alias_left, e.join_var_left), (e.alias_right, e.join_var_right))

        # Mark these columns in each vertex
        self.vertices[e.alias_left].add_join_column(e.join_var_left)
        self.vertices[e.alias_right].add_join_column(e.join_var_right)

    def build_transitive_closure_and_join_pools(self):
        """
        1) Generate transitive edges if multiple aliases share the same union-find rep
           on the same col_name.
        2) Build join_pool_map, i.e. (alias, col_name) -> pool_id.
        """
        # Step 1: Add missing edges (transitive closure)
        self._add_missing_edges()

        # Step 2: Build (alias, col_name) -> pool_id
        self._build_join_pools()

    def _add_missing_edges(self):
        """
        If we have union-find sets that contain more than 2 items
        (alias1.colX, alias2.colX, alias3.colX, ...),
        we add edges among them pairwise if they don't already exist.
        """
        # 1) Gather union-find representatives
        rep_to_items: defaultdict[AliasColPair, list[AliasColPair]] = defaultdict(list)
        # For each vertex, for each join column, gather them by rep
        for alias, vtx in self.vertices.items():
            for col_name in vtx.join_columns:
                # find rep
                rep: AliasColPair = self.uf.find((alias, col_name))
                rep_to_items[rep].append((alias, col_name))

        # 2) We track existing edges in a set to avoid duplicates
        existing_edges: set[tuple[str, str, str, str]] = set()
        for e in self.edges:
            # We'll store them in both directions for easy checking
            existing_edges.add((e.alias_left, e.join_var_left, e.alias_right, e.join_var_right))
            existing_edges.add((e.alias_right, e.join_var_right, e.alias_left, e.join_var_left))

        # 3) For each rep, connect all items pairwise
        for _, item_list in rep_to_items.items():
            # item_list might be [(t, 'id'), (mi, 'movie_id'), (mk, 'movie_id'), ...]
            # We do pairwise edges for them
            n = len(item_list)
            if n <= 1:
                continue
            # pair them up
            for i in range(n):
                for j in range(i + 1, n):
                    a1, c1 = item_list[i]
                    a2, c2 = item_list[j]
                    if (a1, c1, a2, c2) not in existing_edges:
                        # add a new Edge
                        new_e = Edge(a1, a2, c1, c2)
                        self.edges.append(new_e)
                        existing_edges.add((a1, c1, a2, c2))
                        existing_edges.add((a2, c2, a1, c1))

    def _build_join_pools(self):
        """
        Assign a unique pool_id to each union-find representative.
        Then (alias, col_name) -> pool_id.
        """
        rep_to_pool_id = {}
        current_pool = 1

        # We only care about columns that are actually in join_columns
        # For each alias, for each col_name in join_columns:
        for alias, vtx in self.vertices.items():
            self.join_pool_alias_map[alias] = []
            for col_name in vtx.join_columns:
                rep = self.uf.find((alias, col_name))
                if rep not in rep_to_pool_id:
                    rep_to_pool_id[rep] = current_pool
                    current_pool += 1
                # store the mapping
                self.join_pool_map[(alias, col_name)] = rep_to_pool_id[rep]

                # store the mapping
                self.join_pool_alias_map[alias].append(rep_to_pool_id[rep])

        # print(self.join_pool_map)
        # print(self.join_pool_alias_map)

    def __repr__(self):
        vs = "\n".join(str(v) for v in self.vertices.values())
        es = "\n".join(str(e) for e in self.edges)
        pools = "\n".join(f"({k[0]},{k[1]}) -> {v}" for k, v in self.join_pool_map.items())
        return f"JoinGraph:\nVertices:\n{vs}\nEdges:\n{es}\nJoinPools:\n{pools}"
