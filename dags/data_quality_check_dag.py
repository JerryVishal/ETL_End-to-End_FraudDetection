from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def run_quality_checks():
    conn = psycopg2.connect(
        dbname="ecommerce", 
        user="postgres", 
        password="password", 
        host="postgres"
    )
    cur = conn.cursor()
    
    # Check users table is not empty
    cur.execute("SELECT COUNT(*) FROM users;")
    users_count = cur.fetchone()[0]
    if users_count < 1:
        raise ValueError("Data quality check failed: users table is empty!")
    
    # Check that no essential field in users table is null
    cur.execute("SELECT COUNT(*) FROM users WHERE email IS NULL;")
    null_emails = cur.fetchone()[0]
    if null_emails > 0:
        raise ValueError("Data quality check failed: Some users have NULL emails!")
    
    # Add more checks as needed
    print("All data quality checks passed!")
    cur.close()
    conn.close()

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
}

with DAG(
    'data_quality_check_python',
    default_args=default_args,
    schedule_interval='@once',
    description="Run custom data quality checks",
) as dag:

    quality_check_task = PythonOperator(
        task_id='run_quality_checks',
        python_callable=run_quality_checks
    )

    quality_check_task