from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.email import send_email


# Set arguments
default_arguments = {
    'owner': 'chuck',
    'email': 'theraceblogger@comcast.net',
    'email_on_failure': True,
    'start_date': datetime(2021, 10, 27)
}

# Create DAG
update_transform_weather_dag = DAG(
    dag_id='update_transform_weather',
    default_args=default_arguments,
    schedule_interval="15 8 * * 6",
    catchup=False
    # schedule_interval=None
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
