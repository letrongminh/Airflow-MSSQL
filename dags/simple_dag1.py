from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Hàm gửi email thông báo
def email():
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail

    # Cấu hình email đơn giản không có đính kèm
    # message = Mail(
    #     from_email='',
    #     to_emails='',
    #     subject='Hello',
    #     html_content='Hello, all tasks are completed!'
    # )

    # try:
    #     sg = SendGridAPIClient("Send Grid Token here")  # Thay "Send Grid Token here" bằng API Key của SendGrid
    #     response = sg.send(message)
    #     print(f"Email sent successfully with status code: {response.status_code}")
    # except Exception as e:
    #     print(f"Failed to send email: {str(e)}")

# Định nghĩa DAG
dag = DAG(
    'minhlt9',
    default_args={
        'email': [''],
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='A simple DAG sample by MinhLT9',
    schedule_interval="@once",
    start_date=datetime(2024, 10, 8), # Start date
    tags=['minhlt9'],
)

# Task 1: Ghi ngày hiện tại vào file bằng Python
def print_date():
    with open('logs/date.txt', 'w') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

t1 = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag,
)

# Task 2: Tạo độ trễ 5 giây
t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag
)

# Task 3: In thông báo "t3 running"
t3 = BashOperator(
    task_id='echo',
    bash_command='echo t3 running',
    dag=dag
)

# Task gửi email sau khi tất cả các task trên đã hoàn thành
email_operator = PythonOperator(
    task_id='email_operator',
    python_callable=email,
    dag=dag,
)

# Thiết lập thứ tự thực thi các task
[t1, t2] >> t3
