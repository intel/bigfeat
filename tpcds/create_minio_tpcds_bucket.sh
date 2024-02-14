#!/bin/bash

bucket=tpcds-sf1-partitioned-dsdgen-parquet

${CONTAINER_ENGINE} exec -it minio mc alias set myminio http://localhost:9000 minio minio123
${CONTAINER_ENGINE} exec -it minio mc mb myminio/${bucket}