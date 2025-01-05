pushd ..
docker-compose up --remove-orphans -d --build spark-master hive-metastore datanode namenode batch_processing spark-worker hive-server zookeeper hive-metastore-postgresql cassandra