docker exec -it hive-server bash
hive -f /docker-entrypoint-initdb.d/tables.sql
exit