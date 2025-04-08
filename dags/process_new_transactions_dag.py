from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import psycopg2
import json
import traceback

def process_new_transactions():
    try:
        # Connect to Postgres
        conn = psycopg2.connect(
            dbname="ecommerce",
            user="postgres",
            password="password",
            host="postgres"
        )
        cur = conn.cursor()
        
        # Query for one new transaction that hasn't been processed
        cur.execute("""
            SELECT transaction_id, user_id, amount, payment_method, timestamp
            FROM transactions
            WHERE fraud_checked = FALSE
            LIMIT 1;
        """)
        txn = cur.fetchone()
        
        if txn:
            transaction_id, user_id, amount, payment_method, txn_timestamp = txn
            
            # Build payload with transaction details
            payload = {
                "transaction": {
                    "transaction_id": transaction_id,
                    "user_id": str(user_id),
                    "amount": str(amount),
                    "payment_method": payment_method,
                    "timestamp": txn_timestamp.isoformat() if hasattr(txn_timestamp, 'isoformat') else txn_timestamp
                }
            }
            
            # Invoke Lambda using boto3
            client = boto3.client('lambda', region_name='us-east-1')
            response = client.invoke(
                FunctionName='process_transactions_lambda',  # Ensure this exactly matches your Lambda name
                InvocationType='RequestResponse',  # Synchronous invocation
                Payload=json.dumps(payload)
            )
            raw_payload = response['Payload'].read().decode('utf-8')
            print("Raw Lambda response:", raw_payload)
            result = json.loads(raw_payload)
            print("Lambda Response:", result)
            
            # Extract fraud details from Lambda result
            fraud_flag = result.get("fraud", False)
            fraud_reasons = json.dumps(result.get("reasons", []))
            
            # Update the transaction record to mark it as processed with fraud details
            cur.execute(
                """
                UPDATE transactions
                SET fraud_checked = TRUE,
                    fraud_flag = %s,
                    fraud_reasons = %s
                WHERE transaction_id = %s
                """,
                (fraud_flag, fraud_reasons, transaction_id)
            )
            conn.commit()
        else:
            print("No new transactions found.")
        
        cur.close()
        conn.close()
    except Exception as e:
        print("Error in process_new_transactions:")
        print(traceback.format_exc())
        raise

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
}

with DAG(
    'process_new_transactions_dag',
    default_args=default_args,
    schedule_interval='@once',  # For testing; change to '@every_5_minutes' or cron later
    description="Invoke AWS Lambda for fraud detection on new transactions",
) as dag:
    
    process_transactions = PythonOperator(
        task_id='process_new_transactions',
        python_callable=process_new_transactions
    )

    process_transactions