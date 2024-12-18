from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# DAG 1: ETL (every 6 minutes)
dag_etl = DAG(
    'etl_script',
    default_args={'owner': 'user', 'retries': 0},
    schedule_interval='*/10 * * * *',  
    start_date=datetime(2024, 12, 15),
    catchup=False
)

etl_task = BashOperator(
    task_id='run_elt_script',
    bash_command=(
        "/opt/spark/bin/spark-submit --jars ~/.ivy2/jars/aws-java-sdk-bundle-1.11.1026.jar "
        "--packages org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.5.0 "
        "/opt/airflow/automation/elt-script.py"
    ),
    env={
        "SPARK_HOME": "/opt/spark",
        "PATH": "/opt/spark/bin:" + os.environ.get('PATH')
    },
    dag=dag_etl
)

# DAG 2: Data Load Task (every 6 minutes)
dag_load = DAG(
    'load_script',
    default_args={'owner': 'user', 'retries': 0},
    schedule_interval='*/15 * * * *',
    start_date=datetime(2024, 12, 15),
    catchup=False
)

load_task = BashOperator(
    task_id='run_load_script',
    bash_command=(
        "/opt/spark/bin/spark-submit --jars ~/.ivy2/jars/aws-java-sdk-bundle-1.11.1026.jar "
        "--packages org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.5.0 "
        "/opt/airflow/automation/load-script.py"
    ),
    env={
        "SPARK_HOME": "/opt/spark",
        "PATH": "/opt/spark/bin:" + os.environ.get('PATH')
    },
    dag=dag_load
)

# DAG 3: ML Task (every 7 minutes)
dag_ml = DAG(
    'ml_script',
    default_args={'owner': 'user', 'retries': 0},
    schedule_interval='*/30 * * * *',
    start_date=datetime(2024, 12, 15),
    catchup=False
)

ml_task = BashOperator(
    task_id='run_ml_script',
    bash_command=(
        "/opt/spark/bin/spark-submit --jars ~/.ivy2/jars/aws-java-sdk-bundle-1.11.1026.jar "
        "--packages org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.5.0 "
        "/opt/airflow/automation/ml-script.py"
    ),
    env={
        "SPARK_HOME": "/opt/spark",
        "PATH": "/opt/spark/bin:" + os.environ.get('PATH')
    },
    dag=dag_ml
)
