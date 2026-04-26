from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "artem",
}


with DAG(
    dag_id="olympic_datalake_pipeline",
    default_args=default_args,
    start_date=datetime(2026,4,25),
    schedule_interval=None,
    catchup=False
) as dag:


    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command="""
        cd /opt/airflow/goit-de-fp/part2_datalake &&
        python landing_to_bronze.py
        """
    )


    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        cd /opt/airflow/goit-de-fp/part2_datalake &&
        python bronze_to_silver.py
        """
    )


    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
        cd /opt/airflow/goit-de-fp/part2_datalake &&
        python silver_to_gold.py
        """
    )


    landing_to_bronze >> bronze_to_silver >> silver_to_gold