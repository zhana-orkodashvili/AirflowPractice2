import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.sql import SqlSensor
from airflow.operators.email_operator import EmailOperator

def create_contacts_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS contacts (
            id SERIAL PRIMARY KEY,
            name text,
            surname text,
            phone text
    );
    """
    pg_hook.run(create_table_sql)

def insert_test_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    insert_test_data_sql = """
    INSERT INTO contacts (name, surname, phone)
    VALUES
    ('Zhana', 'Orkodashvili', '995123456789'),
    ('John', 'Doe', '995987654321');
    """
    pg_hook.run(insert_test_data_sql)


with DAG(
    dag_id='contacts_load_another',
    start_date=pendulum.today(),
    schedule=None,
    tags=['contacts']
) as dag:
    create_contacts_table_op = PythonOperator(
        task_id='create_contacts_table',
        python_callable=create_contacts_table,
        dag=dag,
    )

    insert_test_data_op = PythonOperator(
        task_id='insert_test_data',
        python_callable=insert_test_data,
        dag=dag,
    )

    data_check_sensor_op = SqlSensor(
        task_id='check_for_data',
        conn_id='postgres_conn_id',
        sql="SELECT COUNT(*) FROM contacts;",
        mode='poke',
        poke_interval=60,
        timeout=7*24*60*60,
        dag=dag,
    )

    email_notification_op = EmailOperator(
        task_id='send_email',
        to='zhana.orkodashvili@gmail.com',
        subject='Airflow Notification',
        html_content='The contacts data has been loaded and inserted into the table successfully.',
        dag=dag,
    )


    create_contacts_table_op >> insert_test_data_op >> data_check_sensor_op >> email_notification_op

