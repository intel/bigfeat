wget "https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz"
wget "https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz"
wget "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"

tar zxvf ./apache-hive-3.1.3-bin.tar.gz
tar zxvf ./hadoop-3.3.6.tar.gz

rm ./hadoop-3.3.6.tar.gz
# rm -rf ./hadoop-3.3.6
rm ./apache-hive-3.1.3-bin.tar.gz
# rm -rf ./apache-hive-3.1.3-bin

chmod +x ./start-metastore
cp ./start-metastore ./apache-hive-3.1.3-bin/bin
cp ./postgresql-42.6.0.jar ./apache-hive-3.1.3-bin/lib
