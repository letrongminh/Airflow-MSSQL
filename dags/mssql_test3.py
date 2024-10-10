'''
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import pymssql

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 10, 10),
#     'email': ['your@email.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5)
# }

# dag = DAG(
#     'mssql_agent_dag',
#     default_args=default_args,
#     description='Execute SQL Server Agent jobs with Airflow',
#     schedule_interval='@once',
#     tags=['minhlt9'],
# )

# # Define the SQL Server connection details
# sql_server_conn = {
#     'server': 'mssqlserver',
#     'user': 'sa',
#     'password': 'pw_123123',
#     'database': 'NVDB'
# }

# def execute_sql_agent_job(job_name):
#     conn = pymssql.connect(
#         server=sql_server_conn['server'],
#         user=sql_server_conn['user'],
#         password=sql_server_conn['password'],
#         database=sql_server_conn['database']
#     )
#     cursor = conn.cursor()

#     # Start the SQL Server Agent job
#     cursor.callproc('msdb.dbo.sp_start_job', (job_name,))
#     conn.commit()

#     # Check the status of each step
#     query = f"SELECT step_id, step_name, current_execution_status FROM msdb.dbo.sysjobsteps WHERE job_id IN (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = '{job_name}')"
#     cursor.execute(query)
#     steps_status = cursor.fetchall()

#     for step in steps_status:
#         step_id, step_name, current_execution_status = step
#         # Your logic to check the status of each step
#         if current_execution_status == 1:  # Success
#             print(f"Step {step_id}: {step_name} executed successfully.")
#         else:
#             print(f"Step {step_id}: {step_name} execution failed.")

#     cursor.close()
#     conn.close()

# t1 = PythonOperator(
#     task_id='execute_sql_agent_job',
#     python_callable=execute_sql_agent_job,
#     op_kwargs={'job_name': 'YourAgentJobName'},
#     dag=dag,
# )

# t1
'''


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pymssql

# Define default params for DAG
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

# Init DAG
dag = DAG(
    'mssql_agent_dag',
    default_args=default_args,
    description='Execute SQL Server Agent jobs with Airflow',
    schedule_interval='@once',
    tags=['minhlt9'],
)

# Define connection to connect to SQL Server
sql_server_conn = {
    'server': 'mssqlserver',
    'user': 'sa',
    'password': 'pw_123123',
    'database': 'NVDB'
}

# Connect to SQL Server
def get_sql_server_connection():
    conn = pymssql.connect(
        server=sql_server_conn['server'],
        user=sql_server_conn['user'],
        password=sql_server_conn['password'],
        database=sql_server_conn['database']
    )
    return conn

# Task: Create customer table
def create_customer_table():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Create Customer table if it isn't existing
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Customer' AND xtype='U')
    CREATE TABLE Customer (
        CustomerID INT PRIMARY KEY IDENTITY(1,1),
        Name NVARCHAR(50),
        Email NVARCHAR(50),
        Phone NVARCHAR(20)
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Customer table created successfully.")

    # Insert data
    insert_data_query = """
    INSERT INTO Customer (Name, Email, Phone) 
    VALUES 
        ('John Doe', 'john.doe@example.com', '123-456-7890'),
        ('Jane Smith', 'jane.smith@example.com', '234-567-8901'),
        ('Michael Johnson', 'michael.johnson@example.com', '345-678-9012')
    """
    cursor.execute(insert_data_query)
    conn.commit()
    print("Sample customer data inserted successfully.")

    cursor.close()
    conn.close()

# Task: Execute job SQL Agent
def execute_sql_agent_job(job_name):
    # use get_sql_server_connection function to connect to SQL Server
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # execute SQL Server Agent job
    cursor.callproc('msdb.dbo.sp_start_job', (job_name,))
    conn.commit()
    print(f"Job {job_name} started successfully.")

    # Check status step in job
    query = f"""
    SELECT step_id, step_name, current_execution_status 
    FROM msdb.dbo.sysjobsteps 
    WHERE job_id IN (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = '{job_name}')
    """
    cursor.execute(query)
    steps_status = cursor.fetchall()

    for step in steps_status:
        step_id, step_name, current_execution_status = step
        if current_execution_status == 1:  # Success
            print(f"Step {step_id}: {step_name} executed successfully.")
        else:
            print(f"Step {step_id}: {step_name} execution failed.")

    cursor.close()
    conn.close()

# Checking taskto connnect to SQL Server)
def check_sql_server_connection():
    conn = get_sql_server_connection()
    if conn:
        print("Successfully connected to SQL Server!")
    conn.close()

# Define task `check_connection` to check connection
check_connection = PythonOperator(
    task_id='check_connection',
    python_callable=check_sql_server_connection,
    dag=dag,
)

# Define task `create_customer_table` to create Table and insert Customer data
create_customer_table_task = PythonOperator(
    task_id='create_customer_table',
    python_callable=create_customer_table,
    dag=dag,
)

# Define task `execute_sql_agent_job` to execute job SQL Agent
execute_job = PythonOperator(
    task_id='execute_sql_agent_job',
    python_callable=execute_sql_agent_job,
    op_kwargs={'job_name': 'YourAgentJobName'},
    dag=dag,
)

# Graph definition
check_connection >> create_customer_table_task >> execute_job
