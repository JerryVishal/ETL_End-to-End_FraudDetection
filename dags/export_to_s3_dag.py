from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 8, 1),
    'catchup': False,
}

with DAG(
    'export_to_s3_dag',
    default_args=default_args,
    schedule_interval=None,  # Only runs when manually triggered
    description="Export aggregated data from Postgres to CSV and upload to S3",
) as dag:
    
    export_product_sales = BashOperator(
        task_id='export_product_sales',
        bash_command="""
            echo "Exporting product_sales table to CSV...";
            PGPASSWORD=password psql -h postgres -U postgres -d ecommerce -c "\\copy product_sales TO '/tmp/product_sales.csv' CSV HEADER";
            echo "CSV export completed. Listing /tmp directory:";
            ls -l /tmp;
            echo "Uploading CSV to S3...";
            aws s3 cp /tmp/product_sales.csv s3://ecommerce-dataset-bucket-1/product_sales.csv;
            echo "Upload complete.";
        """
    )

    export_product_sales