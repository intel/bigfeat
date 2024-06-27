#!/bin/bash


### Modify parameters here ###

SCALE_FACTOR=''
INSTALL_PATH=''
usage () { echo "Arguments : -s <scale_factor> -l <install_path>"; }

while getopts 's:l:' opt; do
    case "${opt}" in
        s ) SCALE_FACTOR=${OPTARG};;
        l ) INSTALL_PATH=${OPTARG};;
        \?) usage; exit 1;;
    esac
done

shift $((OPTIND - 1))
if [ -z ${SCALE_FACTOR} ] && [ -z ${INSTALL_PATH} ]; then
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

# if this scale factor was already run, exit

if test -d "${PRESTO_WORK_DIR}/framework/sf${SCALE_FACTOR}"; then
    echo "Prepare script was already run for this scale factor. Exiting ... "
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

sed "s/sfx/sf${SCALE_FACTOR}/g" create_tables.sql > ./${GENPATH}/create_tables.sql
sed "s/sfx/sf${SCALE_FACTOR}/g" drop_tables.sql   > ./${GENPATH}/drop_tables.sql

##############################

### Copy the queries to presto container ###
PRESTO_TPCDS="${PRESTO_WORK_DIR}/framework/sf${SCALE_FACTOR}"
mkdir -p ${PRESTO_TPCDS}
rm -rf ${PRESTO_TPCDS}/*.sql
cp -r ./${GENPATH}/*.sql ${PRESTO_TPCDS}/

##############################

### Create the tables
printf "\n## Generate the TPC-DS dataset and create the tables ##\n"
./run_query.sh framework/sf${SCALE_FACTOR}/create_tables.sql

# Generate the click log data
printf "\n## Generate the click-log dataset and queries ##\n"
./feature_engineering/clickstream_generator.py \
        --bucket     "tpcds-sf${SCALE_FACTOR}-partitioned-dsdgen-parquet" \
        --schema     "tpcds_sf${SCALE_FACTOR}_parquet" \
        --table      "click_log" \
        --workers    8 \
        --start-date 2450815 \
        --start-time 0 \
        --end-date   2450820 \
        --stop-time  86399

rm -f ./${GENPATH}/*.sql
cp ./feature_engineering/queries/*.sql ./${GENPATH}/

for filename in ./${GENPATH}/*.sql; do
    echo -e "USE ${SCHEMA};\n$(cat $filename)" > $filename
done

cp -r ./${GENPATH}/*.sql ${PRESTO_TPCDS}/
rm -rf ./${GENPATH}
printf "\nPresto Mount Path for Queries: ${PRESTO_TPCDS}\n\n"

##############################

##############################
# Copy run_query and run_and_profile_queries.py to the work directory
cp run_query.sh ${PRESTO_WORK_DIR}
cp run_and_profile_queries.py ${PRESTO_WORK_DIR}
##############################
