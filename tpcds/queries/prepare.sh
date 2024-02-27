#!/bin/bash

### Modify parameters here ###
SCALE_FACTOR=$1
##############################

### Create the minio bucket ###

BUCKET=tpcds-sf${SCALE_FACTOR}

docker exec -it minio mc alias set myminio http://localhost:9000 minio minio123
docker exec -it minio mc mb myminio/${BUCKET}

##############################

### Prepare and generate the Queries ###

SCHEMA=hive.tpcds_sf${SCALE_FACTOR}_parquet
GENPATH=generated

mkdir -p ${GENPATH}
rm -f ${GENPATH}/*.sql
cp raw/*.sql ${GENPATH}/

for filename in ${GENPATH}/*.sql; do
    echo -e "USE ${SCHEMA};\n$(cat $filename)" > $filename
done

sed "s/sfx/sf${SCALE_FACTOR}/g" create_tables.sql > ./generated/create_tables.sql
sed "s/sfx/sf${SCALE_FACTOR}/g" drop_tables.sql > ./generated/drop_tables.sql

##############################

### Copy the queries to presto container ###

PRESTO_TPCDS=$PRESTO_WORK_DIR/tpcds/sf${SCALE_FACTOR}
sudo mkdir $PRESTO_TPCDS
sudo rm -rf ${PRESTO_TPCDS}/*.sql

sudo cp -r ./${GENPATH}/*.sql ${PRESTO_TPCDS}/

##############################