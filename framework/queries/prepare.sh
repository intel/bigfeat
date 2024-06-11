#!/bin/bash


### Modify parameters here ###

SCALE_FACTOR=''
INSTALL_PATH=''
COUNT=0
usage () { echo "Flags : -s <scale_factor> -l <install_path>"; }

while getopts 's:l:' opt; do
    case "${opt}" in
        s ) SCALE_FACTOR=${OPTARG}
            COUNT=$((COUNT+1))
            ;;
        l ) INSTALL_PATH=${OPTARG}
            COUNT=$((COUNT+1))
            ;;
        \?) usage; exit 1;;
    esac
done

shift $((OPTIND - 1))
if [ $COUNT != 2 ]; then
    usage
    exit 1
fi

# Check install path exists
if [ ! -d "${INSTALL_PATH}" ]; then
    echo "Error: ${INSTALL_PATH} does not exist."
    exit 1
fi

PRESTO_WORK_DIR="${INSTALL_PATH}/work"

# Scale factor arg accepts integer only
if ! [[ "$SCALE_FACTOR" =~ ^[0-9]+$ ]]; then
    echo "Error: The scale factor must be an integer."
    exit 1
fi

##############################

### Create the minio bucket ###

BUCKET=tpcds-sf${SCALE_FACTOR}-partitioned-dsdgen-parquet

docker exec -it minio mc alias set myminio http://localhost:9000 minio minio123
docker exec -it minio mc rb --force myminio/${BUCKET}
docker exec -it minio mc mb myminio/${BUCKET}

##############################

### Prepare and generate the Queries ###

SCHEMA=hive.tpcds_sf${SCALE_FACTOR}_parquet
GENPATH=generated

mkdir -p ./${GENPATH}
rm -f ./${GENPATH}/*.sql
cp ./raw/*.sql ./${GENPATH}/
cp ./feature_engineering/*.sql ./${GENPATH}/

for filename in ./${GENPATH}/*.sql; do
    echo -e "USE ${SCHEMA};\n$(cat $filename)" > $filename
done

sed "s/sfx/sf${SCALE_FACTOR}/g" create_tables.sql > ./${GENPATH}/create_tables.sql
sed "s/sfx/sf${SCALE_FACTOR}/g" drop_tables.sql   > ./${GENPATH}/drop_tables.sql

##############################

### Copy the queries to presto container ###
PRESTO_TPCDS="${PRESTO_WORK_DIR}/framework/sf${SCALE_FACTOR}"
echo "Generated path : ${PRESTO_TPCDS}"
mkdir -p ${PRESTO_TPCDS}
rm -rf ${PRESTO_TPCDS}/*.sql
cp -r ./${GENPATH}/*.sql ${PRESTO_TPCDS}/
rm -rf ./${GENPATH}

##############################
