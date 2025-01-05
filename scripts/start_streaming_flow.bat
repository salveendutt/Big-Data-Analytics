pushd ..
docker-compose up --remove-orphans -d --build streaming_processing spark-master nifi kafka spark-worker zookeeper streaming_simulation cassandra 