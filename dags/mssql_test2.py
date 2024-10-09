from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'mssql_test2',
    default_args=default_args,
    schedule_interval=None,
)

create_table_mssql_task = MsSqlOperator(
    task_id="create_table_mssql_task",
    mssql_conn_id='airflow_mssql',
    sql=r"""
    USE master;
    IF OBJECT_ID('Country', 'U') IS NULL
    BEGIN
        CREATE TABLE Country (
            name VARCHAR(255) NOT NULL,
            continent VARCHAR(255) NOT NULL
        );
    END;
    """,
    dag=dag,
)


@dag.task(task_id="insert_mssql_task")
def insert_mssql_hook():
    mssql_hook = MsSqlHook(mssql_conn_id='airflow_mssql', schema='master')
    
    rows = [
        ('India', 'Asia'),
        ('Germany', 'Europe'),
        ('Argentina', 'South America'),
        ('Ghana', 'Africa'),
        ('Japan', 'Asia'),
        ('Namibia', 'Africa'),
    ]
    target_fields = ['name', 'continent']
    mssql_hook.insert_rows(table='Country', rows=rows, target_fields=target_fields)

get_all_countries = MsSqlOperator(
    task_id="get_all_countries",
    mssql_conn_id='airflow_mssql',
    sql=r"""SELECT * FROM Country;""",
    dag=dag,
)

get_countries_from_continent = MsSqlOperator(
    task_id="get_countries_from_continent",
    mssql_conn_id='airflow_mssql',
    sql=r"""SELECT * FROM Country where {{ params.column }}='{{ params.value }}';""",
    params={"column": "CONVERT(VARCHAR, continent)", "value": "Asia"},
    dag=dag,
)

create_table_mssql_task >> insert_mssql_hook() >> get_all_countries >> get_countries_from_continent