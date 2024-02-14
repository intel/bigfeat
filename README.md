# presto-tpcds
Set up environment to run the TPC-DS benchmark on Presto

# Requirements

Docker 25.0.3

## Step 0: Set environment variables

Set PRESTO_WORK_DIR and PRESTO_DATA_DIR in ~/.bashrc.
Run source ~/.bashrc

## Step 1: Run node_setup.sh

Run ./node_setup.sh

This results in the creation of four docker containers: Postgresql, hive, minio, and coordinator

## Step 2: Create TPC-DS bucket

Go to PRESTO_WORK_DIR/tpcds
Run ./create_minio_tpcds_bucket.sh

## Step 3: Create TPC-DS data set

docker exec docker exec -it coordinator /bin/bash
presto-cli --file tpcds/create_tables_tpcds_sf1.sql
