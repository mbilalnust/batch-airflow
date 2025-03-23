from typing import Dict, List, Optional

from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta

from airflow.utils.constants import DEFAULT_ARGS


def get_environment_config() -> Dict[str, str]:
    """
    Returns the environment variables necessary for Docker operators.
    These will be loaded from Airflow Variables.
    """
    return {
        "OPENWEATHER_API_KEY": Variable.get("OPENWEATHER_API_KEY", ""),
        "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY", ""),
        "AWS_REGION": Variable.get("AWS_REGION", ""),
        "S3_BUCKET_NAME": Variable.get("S3_BUCKET_NAME", ""),
        "POSTGRES_HOST": Variable.get("POSTGRES_HOST", ""),
        "POSTGRES_PORT": Variable.get("POSTGRES_PORT", ""),
        "POSTGRES_USER": Variable.get("POSTGRES_USER", ""),
        "POSTGRES_PASSWORD": Variable.get("POSTGRES_PASSWORD", ""),
        "POSTGRES_DB": Variable.get("POSTGRES_DB", "")
    }


def create_dag(
    dag_id: str,
    owner: str = "airflow",
    start_date: datetime = datetime(2023, 1, 1),
    schedule_interval: Optional[str] = None,
    tags: Optional[List[str]] = None,
    catchup: bool = False,
) -> DAG:
    """
    Creates and configures a DAG with standard settings.
    
    Args:
        dag_id: The ID of the DAG.
        owner: The owner of the DAG.
        start_date: The start date for the DAG.
        schedule_interval: The schedule interval for the DAG.
        tags: Tags to apply to the DAG.
        catchup: Whether to catchup on missed DAG runs.
        
    Returns:
        A configured DAG object.
    """
    default_args = DEFAULT_ARGS.copy()
    default_args["owner"] = owner
    default_args["start_date"] = start_date
        
    return DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=catchup,
        tags=tags or [],
    ) 