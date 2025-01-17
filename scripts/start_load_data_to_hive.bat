pushd ..
docker-compose up --remove-orphans -d --build streaming_simulation hive-metastore hive-metastore-postgresql hive-server cassandra nifi spark-master datanode namenode spark-worker zookeeper