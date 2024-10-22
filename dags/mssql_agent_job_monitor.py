from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta
import time

# Define the DAG with default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG object with the default arguments
dag = DAG(
    'mssql_agent_job_monitor',
    default_args=default_args,
    description='A DAG to monitor MSSQL Agent Job',
    schedule_interval=None,
    catchup=False,
    tags=['minhlt9'],
)

# Define the Python function to monitor the status of the job
def monitor_job_status(**kwargs):
    hook = MsSqlHook(mssql_conn_id='airflow_mssql', schema='msdb')
    job_name = kwargs['job_name']
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Get job_id from job_name
    job_id_query = f"SELECT job_id FROM msdb.dbo.sysjobs WHERE name = '{job_name}'"
    cursor.execute(job_id_query)
    job_id = cursor.fetchone()[0]

    # Loop to check job status
    job_completed = False
    while not job_completed:
        job_status_query = f"""
        SELECT 
            ja.run_requested_date,
            ISNULL(ja.stop_execution_date, GETDATE()) AS stop_execution_date,
            DATEDIFF(SECOND, ja.run_requested_date, ISNULL(ja.stop_execution_date, GETDATE())) AS duration,
            CASE 
                WHEN ja.stop_execution_date IS NULL THEN 'Running'
                WHEN h.run_status = 1 THEN 'Succeeded'
                WHEN h.run_status = 0 THEN 'Failed'
                ELSE 'Unknown'
            END AS job_status
        FROM msdb.dbo.sysjobactivity ja
        LEFT JOIN msdb.dbo.sysjobhistory h ON ja.job_id = h.job_id AND ja.job_history_id = h.instance_id
        WHERE ja.job_id = '{job_id}' AND ja.start_execution_date IS NOT NULL
        """
        cursor.execute(job_status_query)
        job_status = cursor.fetchone()

        if job_status:
            run_requested_date, stop_execution_date, duration, status = job_status
            print(f"Job status: {status}, Duration: {duration} seconds")

            if status in ('Succeeded', 'Failed'):
                job_completed = True
        else:
            print("Job status: Running")

        if not job_completed:
            time.sleep(5)  # Wait 5 seconds before checking again

    print(f"Job {job_name} completed with status: {status}")

    cursor.close()
    conn.close()

    return status

# Define the Python functions to print the job status
def on_job_success(**kwargs):
    print(f"MSSQL Agent Job {kwargs['job_name']} completed successfully.")

def on_job_failure(**kwargs):
    print(f"MSSQL Agent Job {kwargs['job_name']} failed.")

# Define the tasks to check the job status and handle success/failure
check_status = PythonOperator(
    task_id='monitor_job_status',
    python_callable=monitor_job_status,
    op_kwargs={'job_name': 'SimpleCustomerJob'},
    dag=dag
)

success_task = PythonOperator(
    task_id='job_success',
    python_callable=on_job_success,
    op_kwargs={'job_name': 'SimpleCustomerJob'},
    trigger_rule='all_success',
    dag=dag
)

failure_task = PythonOperator(
    task_id='job_failure',
    python_callable=on_job_failure,
    op_kwargs={'job_name': 'SimpleCustomerJob'},
    trigger_rule='all_failed',
    dag=dag
)

check_status >> [success_task, failure_task]