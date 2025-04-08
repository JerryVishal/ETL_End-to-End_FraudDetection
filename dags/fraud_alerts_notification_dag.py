from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import psycopg2
import json
import traceback

def check_and_send_alerts():
    """
    Query the transactions table for records where fraud_flag is true and not yet notified.
    For each such record, publish an SNS alert and mark the record as notified.
    """
    try:
        # Connect to Postgres
        conn = psycopg2.connect(
            dbname="ecommerce",
            user="postgres",
            password="password",
            host="postgres"
        )
        cur = conn.cursor()
        
        # Query transactions flagged as fraud and not notified
        cur.execute("""
            SELECT transaction_id, fraud_reasons
            FROM transactions
            WHERE fraud_flag = TRUE AND notified = FALSE;
        """)
        alerts = cur.fetchall()
        
        if not alerts:
            print("No new fraud alerts found.")
            cur.close()
            conn.close()
            return "No fraud alerts."
        
        # Create SNS client
        sns_client = boto3.client('sns', region_name='us-east-1')
        topic_arn = 'arn:aws:sns:us-east-1:177657425307:FraudAlertsTopic'  # Replace YOUR_ACCOUNT_ID
        
        # Process each alert
        for alert in alerts:
            transaction_id, fraud_reasons = alert
            message = f"Fraud alert for transaction {transaction_id}: {fraud_reasons}"
            response = sns_client.publish(
                TopicArn=topic_arn,
                Message=message,
                Subject='Fraud Alert Detected'
            )
            print(f"Published alert for transaction {transaction_id}, SNS response: {response}")
            # Mark this transaction as notified to avoid duplicate alerts
            cur.execute("""
                UPDATE transactions
                SET notified = TRUE
                WHERE transaction_id = %s;
            """, (transaction_id,))
        
        conn.commit()
        cur.close()
        conn.close()
        return f"Processed {len(alerts)} fraud alerts."
    except Exception as e:
        print("Error in check_and_send_alerts:")
        print(traceback.format_exc())
        raise

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG(
    'fraud_alerts_notification_dag',
    default_args=default_args,
    schedule_interval='@hourly',  # Adjust this interval as needed
    description="Check for fraud alerts in the transactions and send SNS notifications",
) as dag:
    
    send_fraud_alerts = PythonOperator(
        task_id='check_and_send_alerts',
        python_callable=check_and_send_alerts
    )
    
    send_fraud_alerts