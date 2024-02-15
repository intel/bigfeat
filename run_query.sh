#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "Arg expected: query sql file"
  echo "Example: ./run_query.sh tpcds/create_tables_tpcds_sf1.sql"
  exit 1
fi

file=$1
docker exec -it coordinator presto-cli --file=/${file}
