from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
}

with DAG(
    'aggregation_dag',
    default_args=default_args,
    schedule_interval=None,  # Run once per day
    description="Aggregate sales and fraud metrics",
) as dag:

    aggregate_sales = PostgresOperator(
        task_id='aggregate_sales',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS product_sales;
            CREATE TABLE product_sales AS
            SELECT p.product_id, p.name, SUM(t.amount) AS total_sales, 
                   COUNT(CASE WHEN t.fraud_flag THEN 1 END) AS fraud_count
            FROM products p
            JOIN transactions t ON p.product_id = t.product_id
            GROUP BY p.product_id, p.name;
        """
    )

    aggregate_sales