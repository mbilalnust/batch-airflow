from datetime import datetime

# Docker image references
WEATHER_ETL_IMAGE = "{{secrets.DOCKERHUB_USERNAME}}/weather-etl:main"

# S3 configuration
S3_BUCKET_NAME = "weather-data-lake"

# Default arguments for DAGs
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "start_date": datetime(2023, 1, 1)
}

# Module paths
BRONZE_MODULE_PATH = "src.bronze"
SILVER_MODULE_PATH = "src.silver"
GOLD_MODULE_PATH = "src.gold" 