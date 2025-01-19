docker compose exec superset superset import-dashboards -p ./dashboard_export.zip -u admin
docker compose exec superset superset import_datasources -p ./datasources_export.zip -u admin