#!/bin/bash

SCHEMA=hive.tpcds_sf1_parquet
GENPATH=generated

mkdir -p ${GENPATH}
rm -f ${GENPATH}/*.sql
cp raw/*.sql ${GENPATH}/

for filename in ${GENPATH}/*.sql; do
    echo -e "USE ${SCHEMA};\n$(cat $filename)" > $filename
done
