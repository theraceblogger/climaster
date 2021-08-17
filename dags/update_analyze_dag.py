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
update_analyze_dag = DAG(
    dag_id='update_analyze',
    default_args=default_arguments,
    schedule_interval=None
)

# Task: update disasters
update_task = BashOperator(
    task_id='update',
    bash_command='jupyter nbconvert --execute --to notebook --inplace /home/ec2-user/climaster/analyze_data/analyze.ipynb',
    dag=update_analyze_dag
)