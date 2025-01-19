import os
# from superset import app


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

# Run the Trino connection initialization
# with app.app_context():
#     from superset.models.core import Database
#     from superset import db
#     session = db.session()

#     # Check if the Trino connection already exists
#     existing_db = session.query(Database).filter_by(database_name="trino").first()
#     if not existing_db:
#         trino_db = Database(
#             database_name="trino",
#             sqlalchemy_uri="trino://admin:@presto:8080/cassandra/fraud_analytics",
#             extra="{}" 
#         )
#         session.add(trino_db)
#         session.commit()


# Cassandra connection configuration via Presto
# ADDITIONAL_DATABASES = {
#     "cassandra": {
#         "sqlalchemy_uri": "trino://admin:@presto:8080/cassandra/fraud_analytics",
#         "allow_ctas": False,
#         "allow_cvas": False,
#         "allow_dml": False,
#         "expose_in_sqllab": True,
#         "extra": {
#             "metadata_params": {},
#             "engine_params": {
#                 "connect_args": {"protocol": "https", "source": "superset"}
#             },
#         },
#     }
# }

# Cache configuration
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
