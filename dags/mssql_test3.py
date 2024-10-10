from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pymssql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 10),
    'email': ['your@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'mssql_agent_dag',
    default_args=default_args,
    description='Execute SQL Server Agent jobs with Airflow',
    schedule_interval='@once',
    tags=['minhlt9'],
)

# Define the SQL Server connection details
sql_server_conn = {
    'server': 'mssqlserver',
    'user': 'sa',
    'password': 'pw_123123',
    'database': 'NVDB'
}

def execute_sql_agent_job(job_name):
    conn = pymssql.connect(
        server=sql_server_conn['server'],
        user=sql_server_conn['user'],
        password=sql_server_conn['password'],
        database=sql_server_conn['database']
    )
    cursor = conn.cursor()

    # Start the SQL Server Agent job
    cursor.callproc('msdb.dbo.sp_start_job', (job_name,))
    conn.commit()

    # Check the status of each step
    query = f"SELECT step_id, step_name, current_execution_status FROM msdb.dbo.sysjobsteps WHERE job_id IN (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = '{job_name}')"
    cursor.execute(query)
    steps_status = cursor.fetchall()

    for step in steps_status:
        step_id, step_name, current_execution_status = step
        # Your logic to check the status of each step
        if current_execution_status == 1:  # Success
            print(f"Step {step_id}: {step_name} executed successfully.")
        else:
            print(f"Step {step_id}: {step_name} execution failed.")

    cursor.close()
    conn.close()

t1 = PythonOperator(
    task_id='execute_sql_agent_job',
    python_callable=execute_sql_agent_job,
    op_kwargs={'job_name': 'YourAgentJobName'},
    dag=dag,
)

t1