# Local Airflow Setup

This repository contains a simple Airflow setup that you can run on your Mac.

## Quick Start Guide

1. Install Airflow and dependencies:
```bash
# Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate

# Install Airflow and dependencies
pip install -r requirements.txt
```

2. Initialize Airflow database:
```bash
# Export the AIRFLOW_HOME environment variable
export AIRFLOW_HOME=$(pwd)

# Initialize the database
airflow db init
```

3. Create an Airflow user (for the web UI):
```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

4. Start Airflow services:
```bash
# Start the scheduler in one terminal
airflow scheduler

# Start the webserver in another terminal
airflow webserver --port 8080
```

5. Access the Airflow web UI at http://localhost:8080

6. Run the example DAG:
```bash
# Manually trigger the DAG
airflow dags trigger hello_world_dag
```

## Example DAG

The `hello_world_dag.py` in the dags folder contains a simple example with two sequential tasks that print "Hello World" messages.

---

2. Set up the following Airflow variables:

```
OPENWEATHER_API_KEY: Your OpenWeather API key
AWS_ACCESS_KEY_ID: Your AWS access key
AWS_SECRET_ACCESS_KEY: Your AWS secret key
AWS_REGION: Your AWS region
S3_BUCKET_NAME: Your S3 bucket name
POSTGRES_HOST: Your PostgreSQL host
POSTGRES_PORT: Your PostgreSQL port
POSTGRES_USER: Your PostgreSQL username
POSTGRES_PASSWORD: Your PostgreSQL password
POSTGRES_DB: Your PostgreSQL database name
```

## Main Workflow

The main DAG (`weather_etl_pipeline`) orchestrates the ETL process through a single task group:

**Weather Analytics**: Runs the complete ETL process with three sequential tasks:
1. Extract raw weather data from the OpenWeather API
2. Transform the data
3. Join and load the data for analytics

The DAG is scheduled to run daily at midnight.

## Custom Operators

This pipeline uses a custom Docker operator:

* **LocalHostDockerOperator**: A wrapper around the standard Airflow DockerOperator that:
  - Automatically fetches all required environment variables from Airflow Variables
  - Sets sensible defaults for Docker execution on localhost
  - Simplifies DAG code by centralizing configuration

This custom operator makes it much easier to maintain Docker-based tasks in the pipeline.

## Docker Integration 