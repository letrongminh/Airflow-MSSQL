import json
from airflow import DAG
from airflow.models.connection import Connection
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

c = Connection(
     conn_id="some_conn",
     conn_type="mysql",
     description="connection description",
     host="myhost.com",
     login="myname",
     password="mypassword",
     extra=json.dumps(dict(this_param="some val", that_param="other val*")),
 )

 dag = DAG(
    'example_mssql',
    schedule_interval='@daily',
    start_date=datetime(2022, 03, 2),
    tags=['example'],
    catchup=False,
)

create_table_mssql_task = MsSqlOperator(
    task_id='create_country_table',
    mssql_conn_id='airflow_mssql',
    sql=r"""
    CREATE TABLE Country (
        country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
        name TEXT,
        continent TEXT
    );
    """,
    dag=dag,
)


populate_user_table = MsSqlOperator(
    task_id='populate_user_table',
    mssql_conn_id='airflow_mssql',
    sql=r"""
            INSERT INTO Users (username, description)
            VALUES ( 'Danny', 'Musician');
            INSERT INTO Users (username, description)
            VALUES ( 'Simone', 'Chef');
            INSERT INTO Users (username, description)
            VALUES ( 'Lily', 'Florist');
            INSERT INTO Users (username, description)
            VALUES ( 'Tim', 'Pet shop owner');
            """,
)

@dag.task(task_id="insert_mssql_task")
	def insert_mssql_hook():
	 mssql_hook = MsSqlHook(mssql_conn_id='airflow_mssql', schema='airflow')
	 
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
)

get_countries_from_continent = MsSqlOperator(
    task_id="get_countries_from_continent",
    mssql_conn_id='airflow_mssql',
    sql=r"""SELECT * FROM Country where {{ params.column }}='{{ params.value }}';""",
    params={"column": "CONVERT(VARCHAR, continent)", "value": "Asia"},
)

create_table_mssql_task > get_all_countries