from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_pascals_triangle(n):
    triangle = [[1]]
    for i in range(1, n):
        row = [1] * (i + 1)
        for j in range(1, i):
            row[j] = triangle[i - 1][j - 1] + triangle[i - 1][j]
        triangle.append(row)
    return triangle

def print_pascals_triangle(triangle):
    for row in triangle:
        print(" ".join(map(str, row)))

def print_triangle_task():
    n = 10 
    triangle = generate_pascals_triangle(n)
    print_pascals_triangle(triangle)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pascal',
    default_args=default_args,
    description='A simple DAG to print Pascal\'s Triangle',
    schedule_interval='44 11 * * *',
    start_date=datetime(2024, 8, 29),
    catchup=False,
)

print_triangle = PythonOperator(
    task_id='print_pascals_triangle',
    python_callable=print_triangle_task,
    dag=dag,
)
