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
import time  # Dùng để chờ giữa các lần kiểm tra trạng thái job

# Định nghĩa các tham số mặc định cho DAG
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

# Khởi tạo DAG
dag = DAG(
    'mssql_agent_dag',
    default_args=default_args,
    description='Execute SQL Server Agent jobs with Airflow',
    schedule_interval='@once',
    tags=['minhlt9'],
)

# Định nghĩa thông tin kết nối tới SQL Server
sql_server_conn = {
    'server': 'mssqlserver',
    'user': 'sa',
    'password': 'pw_123123',
    'database': 'NVDB'
}

# Hàm kết nối tới SQL Server
def get_sql_server_connection():
    conn = pymssql.connect(
        server=sql_server_conn['server'],
        user=sql_server_conn['user'],
        password=sql_server_conn['password'],
        database=sql_server_conn['database']
    )
    return conn

# Task: Tạo bảng và thêm dữ liệu khách hàng
def create_customer_table():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Tạo bảng Customer nếu chưa tồn tại
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

    # Chèn một số dữ liệu mẫu vào bảng Customer
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

# Task: Tạo một SQL Server Agent Job đơn giản
# Hàm tạo SQL Server Agent Job với lịch trình chính xác
def create_sql_agent_job(job_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Tạo SQL Server Agent Job với một bước đơn giản
    create_job_query = f"""
    IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE name = '{job_name}')
    BEGIN
        -- Tạo job mới
        EXEC msdb.dbo.sp_add_job @job_name = '{job_name}';

        -- Thêm bước vào job, ví dụ thực hiện SELECT trên bảng Customer
        EXEC msdb.dbo.sp_add_jobstep 
            @job_name = '{job_name}', 
            @step_name = 'Select from Customer', 
            @subsystem = 'TSQL',
            @command = 'SELECT * FROM NVDB.dbo.Customer',
            @on_success_action = 1, -- Chuyển sang bước tiếp theo nếu thành công
            @on_fail_action = 2; -- Dừng lại nếu thất bại

        -- Tạo lịch chạy job hàng ngày với `@freq_interval = 1`
        EXEC msdb.dbo.sp_add_jobschedule 
            @job_name = '{job_name}', 
            @name = 'DailySchedule', 
            @freq_type = 4,  -- Chạy hàng ngày
            @freq_interval = 1,  -- Chạy mỗi 1 ngày
            @active_start_time = 10000; -- Chạy vào 10:00 sáng

        -- Chỉ định job cho SQL Server Agent
        EXEC msdb.dbo.sp_add_jobserver 
            @job_name = '{job_name}', 
            @server_name = @@SERVERNAME;

        PRINT 'Job created successfully.';
    END
    ELSE
    BEGIN
        PRINT 'Job already exists.';
    END
    """
    cursor.execute(create_job_query)
    conn.commit()
    print(f"SQL Server Agent job '{job_name}' created successfully.")
    
    cursor.close()
    conn.close()


# Task: Thực thi job SQL Agent và kiểm tra trạng thái từng step trong job
def execute_sql_agent_job(job_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Thực thi SQL Server Agent job
    cursor.callproc('msdb.dbo.sp_start_job', (job_name,))
    conn.commit()
    print(f"Job {job_name} started successfully.")

    # Kiểm tra trạng thái của từng bước trong job
    job_id_query = f"SELECT job_id FROM msdb.dbo.sysjobs WHERE name = '{job_name}'"
    cursor.execute(job_id_query)
    job_id = cursor.fetchone()[0]

    # Dùng vòng lặp để kiểm tra trạng thái cho đến khi job hoàn thành
    job_completed = False
    while not job_completed:
        # Truy vấn trạng thái job từ bảng sysjobactivity
        job_status_query = f"""
        SELECT ja.run_status, 
               js.step_id, 
               js.step_name 
        FROM msdb.dbo.sysjobhistory ja
        INNER JOIN msdb.dbo.sysjobs j ON ja.job_id = j.job_id
        INNER JOIN msdb.dbo.sysjobsteps js ON ja.job_id = js.job_id AND ja.step_id = js.step_id
        WHERE ja.job_id = '{job_id}' AND ja.step_id > 0
        ORDER BY ja.run_date DESC, ja.run_time DESC
        """
        cursor.execute(job_status_query)
        step_status = cursor.fetchall()

        # Duyệt qua các step để in ra trạng thái
        for step in step_status:
            run_status, step_id, step_name = step
            if run_status == 1:  # Thành công
                print(f"Step {step_id}: {step_name} executed successfully.")
            elif run_status == 0:  # Thất bại
                print(f"Step {step_id}: {step_name} execution failed.")
            else:
                print(f"Step {step_id}: {step_name} is still running.")

        # Kiểm tra nếu job đã hoàn thành hoặc tất cả các bước đã được thực hiện
        if all(status[0] in (0, 1) for status in step_status):
            job_completed = True
        else:
            time.sleep(5)  # Chờ 5 giây trước khi kiểm tra lại

    print(f"Job {job_name} completed successfully.")

    cursor.close()
    conn.close()

# Task: Kiểm tra kết nối tới SQL Server
def check_sql_server_connection():
    conn = get_sql_server_connection()
    if conn:
        print("Successfully connected to SQL Server!")
    conn.close()

# Định nghĩa các task
check_connection = PythonOperator(
    task_id='check_connection',
    python_callable=check_sql_server_connection,
    dag=dag,
)

create_customer_table_task = PythonOperator(
    task_id='create_customer_table',
    python_callable=create_customer_table,
    dag=dag,
)

create_sql_job_task = PythonOperator(
    task_id='create_sql_agent_job',
    python_callable=create_sql_agent_job,
    op_kwargs={'job_name': 'SimpleCustomerJob'},
    dag=dag,
)

execute_job = PythonOperator(
    task_id='execute_sql_agent_job',
    python_callable=execute_sql_agent_job,
    op_kwargs={'job_name': 'SimpleCustomerJob'},
    dag=dag,
)

# Thiết lập thứ tự các task
check_connection >> create_customer_table_task >> create_sql_job_task >> execute_job
