#!/bin/bash

# Set the install path via runtime arg
INSTALL_PATH=''
COUNT=0
usage () { echo "Mandatory flag : -i <install_path>"; }

while getopts "i:" opt; do
    case ${opt} in
        i ) INSTALL_PATH="${OPTARG}"
            COUNT=$((COUNT+1))
            ;;
        \?) usage
            exit 1
            ;;
    esac
done

shift $((OPTIND - 1))
if [ $COUNT != 1 ]; then
    usage
    exit 1
fi

# Check install path exists
if [ ! -d "${INSTALL_PATH}" ]; then
    echo "Error: ${INSTALL_PATH} does not exist."
    exit 1
fi

# Create the <install_path>/work/framework sub-directory
PRESTO_WORK_DIR=${INSTALL_PATH}/work
mkdir -p ${PRESTO_WORK_DIR}/framework

# Copy config files from repo into work/config sub-directory
sudo cp -r ./config ${PRESTO_WORK_DIR}

# Create the data directory
PRESTO_DATA_DIR=${INSTALL_PATH}/data
mkdir -p ${PRESTO_DATA_DIR}

# make data directories
sudo rm -rf ${PRESTO_DATA_DIR}/minio/*
sudo rm -rf ${PRESTO_DATA_DIR}/postgresql/*
mkdir -p ${PRESTO_DATA_DIR}/minio
mkdir -p ${PRESTO_DATA_DIR}/postgresql
mkdir -p ${PRESTO_DATA_DIR}/postgresql/data

# pull docker containers
docker pull minio/minio
docker pull postgres

# Stop existing containers
docker stop minio; docker rm minio
docker stop postgresql; docker rm postgresql
docker stop hive; docker rm hive
docker stop coordinator; docker rm coordinator

# Run Minio and Docker
docker run -d --privileged -p 9000:9000 -p 9001:9001 \
        --name minio -v ${PRESTO_DATA_DIR}/minio:/data:z \
        -e http_proxy=http://proxy-chain.intel.com:912 \
        -e https_proxy=http://proxy-chain.intel.com:912 \
        quay.io/minio/minio server /data --console-address ":9001"

docker run -itd --privileged -h postgresql \
        -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -p 5433:5432 \
        -v ${PRESTO_DATA_DIR}/postgresql/data:/var/lib/postgresql/data \
        --name postgresql postgres

sleep 5

# create minio access/secret keys
docker exec -it minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
docker exec -it minio mc admin user svcacct add --access-key "minio" --secret-key "minio123" myminio minioadmin

# create postgresql metadata
docker exec -it postgresql psql -U admin -c "drop database metadata"
docker exec -it postgresql psql -U admin -c "create database metadata"

# create hive container
./get_dep.sh
./dockerbuild.sh

# stop minio and postgresql
docker stop minio; docker rm minio
docker stop postgresql; docker rm postgresql

# create docker network
docker network rm --force prestonet
docker network create --subnet=192.168.0.0/16 prestonet

# restart containers as part of prestonet
docker run -d --privileged -p 9000:9000 -p 9001:9001 \
        --net prestonet --name minio \
        -v ${PRESTO_DATA_DIR}/minio:/data:z \
        -e http_proxy=http://proxy-chain.intel.com:912 \
        -e https_proxy=http://proxy-chain.intel.com:912 \
        quay.io/minio/minio server /data --console-address ":9001"

docker run -itd --privileged -h postgresql \
        -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -p 5433:5432 \
        -v ${PRESTO_DATA_DIR}/postgresql/data:/var/lib/postgresql/data \
        --net prestonet --name postgresql postgres

docker run -d --privileged -p 9083:9083/tcp \
        --mount type=bind,source=${PRESTO_WORK_DIR}/config/hive_config/hive-site.xml,target=/opt/apache-hive-3.1.3-bin/conf/hive-site.xml \
        --net prestonet --name hive hive

sleep 10

docker run -d --privileged -p 8080:8080 --net prestonet \
        -v ${PRESTO_WORK_DIR}/framework:/framework:ro \
        -v ${PRESTO_DATA_DIR}/raptorx_cache/data:/data_cache \
        -v ${PRESTO_DATA_DIR}/raptorx_cache/fragment:/fragment_cache \
        --mount type=bind,source=${PRESTO_WORK_DIR}/config/presto_single_node_config/hive.properties,target=/opt/presto-server/etc/catalog/hive.properties \
        --mount type=bind,source=${PRESTO_WORK_DIR}/config/presto_single_node_config/tpcds.properties,target=/opt/presto-server/etc/catalog/tpcds.properties \
        --mount type=bind,source=${PRESTO_WORK_DIR}/config/presto_single_node_config/config.properties,target=/opt/presto-server/etc/config.properties \
        --mount type=bind,source=${PRESTO_WORK_DIR}/config/presto_single_node_config/node.properties,target=/opt/presto-server/etc/node.properties \
        --mount type=bind,source=${PRESTO_WORK_DIR}/config/presto_single_node_config/jvm.config,target=/opt/presto-server/etc/jvm.config \
        --name coordinator prestodb/presto:0.286-edge17 \
        --discovery-uri=http://coordinator:8080 \
        --discovery-server.enabled=true \
        --http-server-port=8080

cd ${PRESTO_WORK_DIR}
