from typing import Dict, List, Optional, Any
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable

class LocalHostDockerOperator(DockerOperator):
    """
    Custom Docker Operator for running containerized tasks on localhost with sensible defaults.
    Automatically fetches environment variables from Airflow Variables.
    """
    
    def __init__(
        self,
        task_id: str,
        command: str,
        image: str = "{{secrets.DOCKERHUB_USERNAME}}/weather-etl:main",
        **kwargs
    ):
        # Get environment variables from Airflow
        env_vars = {
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
        
        # Set default parameters
        default_params = {
            "api_version": "auto",
            "auto_remove": True,
            "environment": env_vars,
            "docker_url": "unix://var/run/docker.sock",
            "network_mode": "bridge"
        }
        
        # Override defaults with any provided values
        default_params.update(kwargs)
        
        super().__init__(
            task_id=task_id,
            image=image,
            command=command,
            **default_params
        ) 