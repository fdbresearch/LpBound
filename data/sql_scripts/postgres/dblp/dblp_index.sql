create index idx_edge_s on dblp.edge using btree(s);
create index idx_edge_t on dblp.edge using btree(t);
create index idx_vertex_i on dblp.vertex using btree(i);
create index idx_vertex_l on dblp.vertex using btree(l);
create index idx_vertex_d on dblp.vertex using btree(d);
analyze dblp.edge;
analyze dblp.vertex;
