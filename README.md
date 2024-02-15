
Set up environment to run the TPC-DS benchmark on Presto

##### Requirements

Docker 25.0.3

##### Step 0: Set environment variables

```shell script
echo 'export PRESTO_WORK_DIR=<presto_work_dir>' >> ~/.bashrc
echo 'export PRESTO_DATA_DIR=<presto_data_dir>' >> ~/.bashrc
source ~/.bashrc
```

##### Step 1: Run node_setup.sh

```shell script
./node_setup.sh
```

This results in the creation of four docker containers: 

* Postgresql 
* hive 
* minio 
* coordinator (presto)

These containers exist within a docker network (prestonet) and can communicate with each other. 

##### Step 2: Create TPC-DS bucket

```shell script
cd PRESTO_WORK_DIR/tpcds
./create_minio_tpcds_bucket.sh
```

##### Step 3: Create TPC-DS data set

```shell script
docker exec docker exec -it coordinator /bin/bash
presto-cli --file tpcds/create_tables_tpcds_sf1.sql
```

##### Script : Run TPC-DS create tables and queries

```shell script
cd PRESTO_WORK_DIR
./run_query.sh tpcds/create_tables_tpcds_sf1.sql
```
