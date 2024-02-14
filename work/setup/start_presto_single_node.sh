docker stop coordinator
docker rm coordinator


docker run -d -p 8080:8080 --net prestonet \
	-v ${PRESTO_WORK_DIR}/tpcds:/tpcds:ro \
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
