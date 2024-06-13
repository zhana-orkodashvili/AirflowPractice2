import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id='covid_regions_load',
    start_date=pendulum.today(),
    schedule=None,
    tags=['covid']
) as dag:
    start_op = EmptyOperator(task_id='start')

    load_covid_regions_op = BashOperator(
        task_id='load_covid_regions',
        bash_command='process_regions.sh',
        env={'message': 'Starting a new DAG'}
    )

    finish_op = EmptyOperator(task_id='finish')

    start_op >> load_covid_regions_op >> finish_op