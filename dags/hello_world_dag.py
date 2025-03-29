from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    'hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define a function that will be executed by the PythonOperator
def print_hello():
    print("Hello World!")
    return "Hello World!"

# Create the task
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Set the task dependencies (in this simple case, there's just one task)
# If we add more tasks later, we can set dependencies like: task1 >> task2 