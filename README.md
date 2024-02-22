
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

##### Step 2: Run prepare.sh

Prepare.sh (i) creates the minio bucket corresponding to the scale factor, (ii) generates sql queries (create_sfx_tables.sql and q1.sql through q99.sql), and (iii) put them under the directory 'tpcds/sfx/'. This directory is mapped to the presto docker container and can be accessed from within it.


You can modify the SCALE_FACTOR in the prepare.sh file, by default it is 1.

```shell script
cd tpcds/queries
./prepare.sh
```

##### Step 3: Run queries

Here is an example on how to run query 1 (sfx is sf1 for SCALE_FACTOR 1 etc.).

```shell script
docker exec -it coordinator /bin/bash
presto-cli --file ./tpcds/sf1/create_sfx_tables.sql
presto-cli --file ./tpcds/sf1/q1.sql
```








<!-- ##### Step 3: Create TPC-DS data set

```shell script
docker exec docker exec -it coordinator /bin/bash
presto-cli --file tpcds/create_tables_tpcds_sf1.sql
```

##### Script : Run TPC-DS create tables and queries

```shell script
cd PRESTO_WORK_DIR
./run_query.sh tpcds/create_tables_tpcds_sf1.sql
``` -->
