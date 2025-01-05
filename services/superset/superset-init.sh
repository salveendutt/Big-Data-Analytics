#!/bin/bash

set -e

echo "Setting up permissions..."
chown -R superset:superset /app/superset_home
chmod 777 /app/superset_home

echo "Switching to superset user for initialization..."
su superset << EOF

echo "Initializing Superset..."

# Create an admin user
superset fab create-admin \
    --username "\$ADMIN_USERNAME" \
    --firstname Superset \
    --lastname Admin \
    --email "\$ADMIN_EMAIL" \
    --password "\$ADMIN_PASSWORD"

# Initialize the database
superset db upgrade

# Setup roles and permissions
superset init

EOF

echo "Starting Superset server..."
exec su superset -c '
gunicorn \
    --bind "0.0.0.0:8088" \
    --workers 10 \
    --timeout 120 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
'