CREATE SCHEMA dblp;

-- Edge
CREATE TABLE dblp.edge (
s INTEGER NOT NULL,
t INTEGER NOT NULL,
PRIMARY KEY (s, t)
);

-- Vertex
CREATE TABLE dblp.vertex (
    i INTEGER PRIMARY KEY,
    l INTEGER ,
    d INTEGER
);
