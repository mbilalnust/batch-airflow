from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from utils.constants import (
    IMAGE_BATCH_PIPELINE,
    BATCH_PIPELINE_SILVER_RPATH,
    BATCH_PIPELINE_SILVER_DIR
)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'analytics_dag',
    default_args=default_args,
    description='ETL pipeline for analytics data processing',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['analytics', 'etl'],
)

with dag:
    # Start the pipeline
    start = EmptyOperator(task_id='start_pipeline')
    
    # Empty operator to preserve execution time
    time_preserver = EmptyOperator(task_id='time_preserver')
    
    # Current date in KST (Korea Standard Time) - this variable will be available for all tasks
    # Use a Jinja template that will be rendered at runtime
    KST_TODAY_DATE = "{{ (task_instance.start_date + macros.timedelta(hours=9)).strftime('%Y-%m-%d') }}"
    
    # Define common arguments for ETL jobs
    common_args = ["--env", "dev", "--from-date", KST_TODAY_DATE, "--to-date", KST_TODAY_DATE]
    
    # Task group for data extraction processes
    with TaskGroup(group_id="data_extraction") as extraction_group:
        # Extract weather data
        extract_weather = DockerOperator(
            task_id='extract_weather_data',
            image=IMAGE_BATCH_PIPELINE,
            command=f"python {BATCH_PIPELINE_SILVER_RPATH}/extract_weather_data.py {' '.join(common_args)}",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
        )
        
        # Extract user activity data
        extract_user_activity = DockerOperator(
            task_id='extract_user_activity',
            image=IMAGE_BATCH_PIPELINE,
            command=f"python {BATCH_PIPELINE_SILVER_RPATH}/extract_user_activity.py {' '.join(common_args)}",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
        )
    
    # Task group for data transformation processes
    with TaskGroup(group_id="data_transformation") as transformation_group:
        # Transform weather data
        transform_weather = DockerOperator(
            task_id='transform_weather_data',
            image=IMAGE_BATCH_PIPELINE,
            command=f"python {BATCH_PIPELINE_SILVER_RPATH}/transform_weather_data.py {' '.join(common_args)}",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
        )
        
        # Transform user activity data
        transform_user_activity = DockerOperator(
            task_id='transform_user_activity',
            image=IMAGE_BATCH_PIPELINE,
            command=f"python {BATCH_PIPELINE_SILVER_RPATH}/transform_user_activity.py {' '.join(common_args)}",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
        )
    
    # Task group for data loading processes
    with TaskGroup(group_id="data_loading") as loading_group:
        # Load transformed data to database
        load_to_database = DockerOperator(
            task_id='load_to_database',
            image=IMAGE_BATCH_PIPELINE,
            command=f"python {BATCH_PIPELINE_SILVER_RPATH}/load_to_database.py {' '.join(common_args)}",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
        )
        
        # Generate reports
        generate_reports = DockerOperator(
            task_id='generate_reports',
            image=IMAGE_BATCH_PIPELINE,
            command=f"python {BATCH_PIPELINE_SILVER_RPATH}/generate_reports.py {' '.join(common_args)}",
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
        )
    
    # Define a final task
    end = EmptyOperator(task_id='end_pipeline')
    
    # Set up task dependencies
    start >> time_preserver >> extraction_group >> transformation_group >> loading_group >> end 