docker compose exec superset superset export-dashboards -f dashboard_export.zip && docker cp superset:/app/dashboard_export.zip ../services/superset