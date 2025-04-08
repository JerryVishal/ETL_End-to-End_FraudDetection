import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def load_csv_to_postgres(table, csv_path):
    # Column mappings that match your CSV headers
    table_columns = {
    'users': ('user_id', 'name', 'email', 'location', 'signup_date'),
    'products': ('product_id', 'name', 'category', 'price', 'stock'),
    'transactions': ('transaction_id', 'user_id', 'product_id', 'amount', 'payment_method', 'status', 'timestamp'),
    'user_interactions': ('interaction_id', 'user_id', 'product_id', 'action', 'timestamp'),
    'payments': ('transaction_id', 'payment_method', 'card_type', 'status')
}
    try:
        conn = psycopg2.connect(
            dbname="ecommerce",
            user="postgres",
            password="password",
            host="postgres"
        )
        cur = conn.cursor()
        with open(csv_path, 'r') as f:
            sql = f"""
            COPY {table} ({','.join(table_columns[table])})
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
            """
            cur.copy_expert(sql, f)
        conn.commit()
        cur.close()
        print(f"Loaded data from {csv_path} into table {table}.")
    except Exception as e:
        print(f"Error loading {csv_path}: {e}")
    finally:
        if conn:
            conn.close()

def load_all_data():
    base_path = '/opt/airflow/synthetic_data'
    files_and_tables = {
        'users.csv': 'users',
        'products.csv': 'products',
        'transactions.csv': 'transactions',
        'user_interactions.csv': 'user_interactions',
        'payments.csv': 'payments'
    }
    for file_name, table in files_and_tables.items():
        csv_path = os.path.join(base_path, file_name)
        load_csv_to_postgres(table, csv_path)

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG(
    'csv_to_postgres',
    default_args=default_args,
    schedule_interval=None,
    description="Load synthetic CSV data into PostgreSQL",
) as dag:
    
    load_data_task = PythonOperator(
        task_id='load_csv_files',
        python_callable=load_all_data
    )

    load_data_task