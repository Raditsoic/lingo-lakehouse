from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag_etl = DAG(
    'etl_script',
    default_args={'owner': 'user', 'retries': 1},
    schedule_interval='*/5 * * * *',  
    start_date=datetime(2024, 12, 15),
    catchup=False
)

etl_task = BashOperator(
    task_id='run_elt_script',
    bash_command='python /opt/airflow/automation/elt-script.py',
    dag=dag_etl
)

# DAG 2: Data Load Task (every 6 minutes)
dag_load = DAG(
    'load_script',
    default_args={'owner': 'user', 'retries': 1},
    schedule_interval='*/6 * * * *',
    start_date=datetime(2024, 12, 15),
    catchup=False
)

load_task = BashOperator(
    task_id='run_load_script',
    bash_command='python /opt/airflow/automation/load-script.py',
    dag=dag_load
)

# DAG 3: ML Task (every 7 minutes)
dag_ml = DAG(
    'ml_script',
    default_args={'owner': 'user', 'retries': 1},
    schedule_interval='*/7 * * * *',
    start_date=datetime(2024, 12, 15),
    catchup=False
)

ml_task = BashOperator(
    task_id='run_ml_script',
    bash_command='python /opt/airflow/automation/ml-script.py',
    dag=dag_ml
)
