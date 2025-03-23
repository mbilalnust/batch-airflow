from datetime import datetime

from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from airflow.utils.constants import BRONZE_MODULE_PATH, SILVER_MODULE_PATH, GOLD_MODULE_PATH
from airflow.utils.dags import create_dag
from airflow.utils.operators import LocalHostDockerOperator

# Create the main ETL DAG
with create_dag(
    dag_id="weather_etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 0 * * *",  # Daily at midnight
    tags=["weather", "etl"],
) as dag:
    
    # Start task
    start = EmptyOperator(task_id="start")
    
    # End task
    end = EmptyOperator(task_id="end")
    
    # Weather analytics task group
    with TaskGroup(
        group_id="weather_analytics",
        tooltip="Process weather data through ETL pipeline"
    ) as weather_analytics:
        
        # Weather data extraction task
        weather_data_extraction = LocalHostDockerOperator(
            task_id="weather_data_extraction",
            command=f"python -m {BRONZE_MODULE_PATH}.weather_data_pipeline"
        )
        
        # Weather data transformation task
        weather_transform = LocalHostDockerOperator(
            task_id="weather_transform",
            command=f"python -m {SILVER_MODULE_PATH}.weather_transform_pipeline"
        )
        
        # City weather join task
        city_weather_join = LocalHostDockerOperator(
            task_id="city_weather_join",
            command=f"python -m {GOLD_MODULE_PATH}.city_weather_join_pipeline"
        )
        
        # Set task dependencies within the group
        weather_data_extraction >> weather_transform >> city_weather_join
    
    # Set main task dependencies
    start >> weather_analytics >> end 