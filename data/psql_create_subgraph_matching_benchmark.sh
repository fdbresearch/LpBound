#!/bin/bash

createdb dblp
psql dblp -f sql_scripts/postgres/dblp/dblp.sql
psql dblp -f sql_scripts/postgres/dblp/dblp_index.sql
psql dblp -f sql_scripts/postgres/dblp/dblp_load.sql
