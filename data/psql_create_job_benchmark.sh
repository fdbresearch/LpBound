#!/bin/bash

# create the database
createdb imdb
psql imdb -f sql_scripts/postgres/imdb/schema.sql
psql imdb -f sql_scripts/postgres/imdb/imdb_create.sql
psql imdb -f sql_scripts/postgres/imdb/fkindexes.sql
psql postgres -f sql_scripts/postgres/imdb/CreateJOBLightDB.sql
psql postgres -f sql_scripts/postgres/imdb/CreateJOBLightRangesDB.sql
psql postgres -f sql_scripts/postgres/imdb/CreateJOBMDB.sql
