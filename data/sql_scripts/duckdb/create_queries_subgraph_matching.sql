CREATE SCHEMA dblp;

-- Create the dblp.vertex table
CREATE TABLE dblp.vertex (
    i INT PRIMARY KEY,
    l INT NOT NULL,
    d INT NOT NULL
);

-- Create the dblp.edge table
CREATE TABLE dblp.edge (
    s INT NOT NULL,
    t INT NOT NULL,
    PRIMARY KEY (s, t)
);
