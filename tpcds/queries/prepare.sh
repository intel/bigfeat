#!/bin/bash

### Default to scale factor of 1 if not specified

if [[ $# -eq 0 ]]; then
    SCALE_FACTOR=1
else
    SCALE_FACTOR=$1
fi

if ! [[ "$SCALE_FACTOR" =~ ^[0-9]+$ ]]; then
  echo "Error: The scale factor must be an integer."
  exit 1
fi
##############################

### Create the minio bucket ###

BUCKET=tpcds-sf${SCALE_FACTOR}-partitioned-dsdgen-parquet

docker exec -it minio mc alias set myminio http://localhost:9000 minio minio123
docker exec -it minio mc mb myminio/${BUCKET}

##############################

### Prepare and generate the Queries ###

SCHEMA=hive.tpcds_sf${SCALE_FACTOR}_parquet
GENPATH=generated

mkdir -p ${GENPATH}
rm -f ${GENPATH}/*.sql
cp raw/*.sql ${GENPATH}/
cp feature_engineering/*.sql ${GENPATH}/

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
