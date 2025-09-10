#!/bin/bash

createdb stats
psql stats -f sql_scripts/postgres/stats/stats.sql
psql stats -f sql_scripts/postgres/stats/stats_index.sql
psql stats -f sql_scripts/postgres/stats/stats_load.sql
