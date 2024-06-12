# Feature Engineering Framework

<div align="justify">
This repository contains a feature engineering framework that closely captures the scale, data types, and query workloads of feature engineering pipelines at various large-scale companies.
</div>


#### License

* TBD


#### Requirements

The following packages are needed:
* Docker 25.0.3
* OpenJDK 1.8.0

> [!IMPORTANT]
> Set up docker to run without sudo access. Please refer to this post-installation
  [**guide**](https://docs.docker.com/engine/install/linux-postinstall/)

The following python packages are needed:
* numpy
* sqlalchemy
* 'pyhive[presto]'
* pandas
* sql_formatter
* Minio
* pyarrow


#### Step 1: Run node_setup.sh

```shell script
./node_setup.sh -i <install_path>
```
<install_path> is the directory where we map volumes for various docker containers. We recommend that this directory exist outside the repo.

This results in the creation of four docker containers: 
* Postgresql 
* hive 
* minio 
* coordinator (presto)

These containers exist within a docker network (prestonet) and can communicate with each other. 

#### Step 2: Run prepare.sh

* Creates the minio bucket corresponding to the scale factor
* Generates sql queries (create_sfx_tables.sql and q1.sql through q99.sql)
* Put them under the directory 'framework/sfx/'. This directory is mapped to the presto docker container
  and can be accessed from within it.

You can modify the scale factor in the prepare.sh file: 1, 10, 100,...

```shell script
cd framework/queries
./prepare.sh -s <scale_factor> -l <install_path>
```

<install_path> is the same path as provided in Step 1.

#### Step 3: Run queries

Here is an example on how to run query 1 (sfx is sf1 for SCALE_FACTOR 1 etc.).

```shell script
docker exec -it coordinator /bin/bash
presto-cli --file ./framework/sf1/create_tables.sql
presto-cli --file ./framework/sf1/q1.sql
```

---
**NOTE:**

Run queries using the following script:

```shell script
cd <install_path>/work
<repo_path>/framework/queries/run_query.sh framework/sf1/create_tables.sql
<repo_path>/framework/queries/run_query.sh framework/sf1/q1.sql
```
---

