import os

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ENABLE_EXPLORE_JSON_CSRF_PROTECTION": True,
    "ENABLE_DASHBOARD_CROSS_FILTERS": True,
}

# Basic Superset configs
ENABLE_PROXY_FIX = True
SECRET_KEY = "YOUR_OWN_RANDOM_GENERATED_STRING"
ROW_LIMIT = 5000

# Database configuration
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
SQLALCHEMY_TRACK_MODIFICATIONS = True

# Cassandra connection configuration via Presto
ADDITIONAL_DATABASES = {
    "cassandra": {
        "sqlalchemy_uri": "trino://admin:@presto:8080/cassandra/fraud_analytics",
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "expose_in_sqllab": True,
        "extra": {
            "metadata_params": {},
            "engine_params": {
                "connect_args": {"protocol": "https", "source": "superset"}
            },
        },
    }
}

# Cache configuration
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
