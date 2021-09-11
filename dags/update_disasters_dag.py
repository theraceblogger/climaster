from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.email import send_email

# Set arguments
default_arguments = {
    'owner': 'chuck',
    'email': 'theraceblogger@comcast.net',
    'email_on_failure': True,
    'start_date': datetime(2021, 9, 1)
}

# Create DAG
update_transform_disasters_dag = DAG(
    dag_id='update_transform_disasters',
    default_args=default_arguments,
    schedule_interval="0 1 * * 6"
)

# Task: update disasters
update_task = BashOperator(
    task_id='update',
    bash_command='python3 /home/ec2-user/climaster/update_databases/update_disasters.py ',
    dag=update_transform_disasters_dag
)

# Task: transform disasters
transform_task = BashOperator(
    task_id='transform',
    bash_command='python3 /home/ec2-user/climaster/transform_data/transform_disasters.py ',
    dag=update_transform_disasters_dag
)

# Dependencies
update_task >> transform_task
