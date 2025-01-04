docker exec -it spark-worker bash
pip install numpy
exit

docker exec -it hive-server bash
hive -f /docker-entrypoint-initdb.d/tables.sql
exit