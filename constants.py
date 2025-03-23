# In a constants.py file in your Airflow repo
from airflow.models import Variable

# Image reference
IMAGE_WEATHER_ETL = f"yourusername/weather-etl:{Variable.get('IMAGE_VERSION_WEATHER_ETL', 'latest')}"

# Paths
WEATHER_ETL_SOURCE_CODE_ROOT = "src"
WEATHER_ETL_BRONZE_RPATH = f"{WEATHER_ETL_SOURCE_CODE_ROOT}/bronze"
WEATHER_ETL_SILVER_RPATH = f"{WEATHER_ETL_SOURCE_CODE_ROOT}/silver" 
WEATHER_ETL_GOLD_RPATH = f"{WEATHER_ETL_SOURCE_CODE_ROOT}/gold"