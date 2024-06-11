# Feature Engineering Framework

<div align="justify">
Feature  Engineering is integral to all machine learning pipelines. It is the process through which these
pipelines  transform  raw data  into features that  are then stored and used for several state-of-the-art
machine learning tasks, for  instance, training and  inferring from deep learning  recommendation  models
(DLRMs) and  fine-tuning NLP  models  through  retrieval-augmented generation (RAG).  Feature engineering
consumes a significant portion of resources at large-scale companies; for instance, it  accounts for over
30 percent of the power consumption of  production DLRM pipelines at Meta. Feature  engineering pipelines
at large-scale companies such as Meta, Pinterest, and Amazon exhibit unique properties including a highly
disaggregated hardware/software  stack, specialized nested data types, massive  data about  user browsing
history, and high refresh rate. Existing benchmarks such as TPC-DS or FEBench do not capture all of these
characteristics within the same benchmark. This repository presents a feature  engineering framework that
closely  captures the  scale, data types, and query  workloads of data  pipelines at  various large-scale
companies.

Follow the instructions below to set up the Feature Engineering Framework.
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

#### Step 3: Run queries

Here is an example on how to run query 1 (sfx is sf1 for SCALE_FACTOR 1 etc.).

```shell script
docker exec -it coordinator /bin/bash
presto-cli --file ./framework/sf1/create_sf1_tables.sql
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

