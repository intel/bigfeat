# **Big-Feat: Feature Engineering Framework**

<div align="justify">
This repository contains Big-feat, a feature engineering framework that closely captures the scale, data types, and query
workloads of feature engineering pipelines at various large-scale companies.<br />
<br />
We narrow the scope to the e-Commerce scenario in TPC-DS and introduce User Browser History so that our data
generator can produce the interaction between features and user-behaviors within a user-session on a sequential
timeline.
</div>


### **License**

<div align="justify">
The SQL table creation and dataset derive from material licensed as follows:<br />
TPC Benchmark™ and TPC-DS are trademarks of the Transaction Processing Performance Council.<br />
All parties are granted permission to copy and distribute to any party without fee all or part of this material 
provided that:<br />
1. copying and distribution is done for the primary purpose of disseminating TPC material;<br />
2. the TPC copyright notice, the title of the publication, and its date appear, and notice is given that copying 
is by permission of the Transaction Processing Performance Council.<br />
<br />
This feature engineering framework codebase is licensed under Apache License as follows:<br />
Licensed under the Apache License, Version 2.0 (the "License");<br />
you may not use this file except in compliance with the License.<br />
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
</div>


### **Requirements**

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


### **Step 1: Run node_setup.sh**

```shell script
./node_setup.sh -i <install_path>
```
<install_path> is the directory where we map volumes for various docker containers. We recommend that this
directory exist outside the repo.

This results in the creation of four docker containers: 
* Postgresql 
* hive 
* minio 
* coordinator (presto)

These containers exist within a docker network (prestonet) and can communicate with each other. 

### **Step 2: Run prepare.sh**

* Creates the minio bucket corresponding to the scale factor.
* Generates sql queries (create_sfx_tables.sql and q1.sql through q99.sql)
* Put them under the directory 'framework/sfx/'. This directory is mapped to the presto docker container
  and can be accessed from within it.
* Generates the TPC-DS dataset and tables.
* Generates the click log dataset based on the TPC-DS tables.
* Creates the create and drop table sql query files for click log dataset. Put these files under the
  directory 'framework/sfx/'.

You can modify the scale factor in the prepare.sh file: 1, 10, 100,...

```shell script
cd framework/queries
./prepare.sh -s <scale_factor> -l <install_path>
```

<install_path> is the same path as provided in Step 1.

### **Step 3: Create click log table and run other queries**

Run the create click log table query (sfx is sf1 for SCALE_FACTOR 1 etc.):

```shell script
docker exec -it coordinator /bin/bash
presto-cli --file /framework/sf1/create_click_log_table.sql
```

> [!NOTE]
> Run queries using the following script:
>
> ```shell script
> cd <install_path>/work
> <repo_path>/framework/run_query.sh framework/sf1/create_click_log_table.sql
> <repo_path>/framework/run_query.sh framework/sf1/q1.sql
> ```

