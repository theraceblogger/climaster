from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Set arguments
default_arguments = {
    'owner': 'chuck',
    'email': 'theraceblogger@comcast.net',
    'catchup': False,
    'start_date': datetime(2021, 1, 1)
}

# Create DAG
update_transform_disasters_dag = DAG(
    dag_id='update_transform_disasters',
    default_args=default_arguments,
    schedule_interval=None
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
