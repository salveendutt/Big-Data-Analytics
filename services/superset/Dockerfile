FROM apache/superset:latest

USER root

# Install additional dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        libffi-dev \
        python3-dev \
        default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages for database connectivity
RUN pip install --no-cache \
    trino \
    sqlalchemy-trino

# Create directory for SQLite database with proper permissions
RUN mkdir -p /app/superset_home && \
    chown -R superset:superset /app/superset_home && \
    chmod 777 /app/superset_home

# Copy configuration and initialization files
COPY ./superset_config.py /app/pythonpath/
COPY ./superset-init.sh /app/superset-init.sh
RUN chmod +x /app/superset-init.sh

ENV SUPERSET_CONFIG_PATH /app/pythonpath/superset_config.py

EXPOSE 8088

CMD ["/app/superset-init.sh"]