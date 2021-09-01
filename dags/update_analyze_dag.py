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
update_analyze_dag = DAG(
    dag_id='update_analyze',
    default_args=default_arguments,
    schedule_interval="0 3 * * 6"
)

# Task: update analyze
update_task = BashOperator(
    task_id='update',
    bash_command='jupyter nbconvert --execute --to notebook --inplace /home/ec2-user/climaster/analyze_data/analyze.ipynb',
    dag=update_analyze_dag
)

update_task