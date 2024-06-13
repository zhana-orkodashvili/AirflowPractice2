import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id='contacts_load',
    start_date=pendulum.today(),
    schedule=None,
    tags=['contacts']
) as dag:

    create_contacts_table = PostgresOperator(
        task_id='create_contacts_table',
        postgres_conn_id='postgres_conn_id',
        sql='create_contacts_table.sql',
        dag=dag,
    )

    insert_test_data = PostgresOperator(
        task_id='insert_test_data',
        postgres_conn_id='postgres_conn_id',
        sql='SELECT insert_test_data();',
        dag=dag,
    )

    create_contacts_table >> insert_test_data