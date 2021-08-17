from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago

# Set arguments
default_arguments = {
    'owner': 'chuck',
    'email': 'theraceblogger@comcast.net',
    'email_on_failure': True,
    'catchup': False,
    'start_date': days_ago(1)
}

# Create DAG
update_transform_weather_dag = DAG(
    dag_id='update_transform_weather',
    default_args=default_arguments,
    schedule_interval=None
)

# Task: update weather
update_task = BashOperator(
    task_id='update',
    bash_command='python3 /home/ec2-user/climaster/update_databases/update_weather.py ',
    dag=update_transform_weather_dag
)

# Task: transform weather
transform_task = BashOperator(
    task_id='transform',
    bash_command='python3 /home/ec2-user/climaster/transform_data/transform_weather.py ',
    dag=update_transform_weather_dag
)

# Dependencies
update_task >> transform_task
