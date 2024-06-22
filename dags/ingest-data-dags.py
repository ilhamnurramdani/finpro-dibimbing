import pandas as pd 
from datetime import datetime
import os
import glob
import fastavro
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError


# Define the get_conn function
def get_conn():
    connection_string = 'postgresql://postgres:postgres@postgres-db:5432/postgres'   
    engine = create_engine(connection_string)
    return engine.connect()

# Define the ingest_data_csv function
def ingest_data_csv():
    conn = get_conn()
    files = glob.glob('data/customer_*.csv')
    for file in files:
        df = pd.read_csv(file)
        df.to_sql('customers', conn, if_exists='append', index=False)
    conn.close()

# Define the ingest_data_json_login_attempts function
def ingest_data_json_login_attempts():
    conn = get_conn()
    files = glob.glob('data/login_attempts_*.json')
    for file in files :
        df = pd.read_json(file)
        df.to_sql('login_attempts', conn, if_exists='append', index=False)
    conn.close()

# Define the ingest_data_json_coupons function
def ingest_data_json_coupons():
    conn = get_conn()
    files = glob.glob('data/coupons.json')
    for file in files :
        df = pd.read_json(file)
        df.to_sql('coupons', conn, if_exists='append', index=False)
    conn.close()

# Define the ingest_data_xls_products function
def ingest_data_xls_products():
    conn = get_conn()
    files = glob.glob('data/product.xls')
    for file in files :
        df = pd.read_excel(file)
        df.to_sql('products', conn, if_exists='append', index=False)
    conn.close()

# Define the ingest_data_xls_product_category function
def ingest_data_xls_product_category():
    conn = get_conn()
    files = glob.glob('data/product_category.xls')
    for file in files :
        df = pd.read_excel(file)
        df.to_sql('product_category', conn, if_exists='append', index=False)
    conn.close()

# Define the ingest_data_xls_supplier function
def ingest_data_xls_supplier():
    conn = get_conn()
    files = glob.glob('data/supplier.xls')
    for file in files :
        df = pd.read_excel(file)
        df.to_sql('supplier', conn, if_exists='append', index=False)
    conn.close()

# Define the ingest_data_avro_orders function
def ingest_data_parquet_order(): 
    conn = get_conn()
    files = glob.glob('data/order.parquet')
    for file in files :
        df = pd.read_parquet(file)
        df.to_sql('orders', conn, if_exists='append', index=False)
    conn.close()

# Define the ingest_data_avro_order_items function
def ingest_data_avro_order_items():
    conn = get_conn()
    files = [f for f in os.listdir('data/') if f.endswith('.avro')]
    for file in files :
        file_path = os.path.join('data/', file)
        with open(file_path, 'rb') as f:
            reader = fastavro.reader(f)
            record = [record for record in reader]
            df = pd.DataFrame(record)
            df.to_sql('order_items', conn, if_exists='append', index=False)
    conn.close()

# Define the default arguments for the DAG
default_args = {
    "owner" : "Kelompok 3",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Define the DAG
dag = DAG(
    "ingest_data",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
)
# Define the PythonOperator task
csv = PythonOperator(
    task_id="ingest_data_csv",
    python_callable=ingest_data_csv,
    dag=dag,
)
json_login_attempts = PythonOperator(
    task_id="ingest_data_json_login_attempts",
    python_callable=ingest_data_json_login_attempts,
    dag=dag,
)
json_coupons = PythonOperator(
    task_id="ingest_data_json_coupons",
    python_callable=ingest_data_json_coupons,
    dag=dag,
)

xls_products_category = PythonOperator(
    task_id="ingest_data_xls_product_category",
    python_callable=ingest_data_xls_product_category,
    dag=dag,
)
xls_product = PythonOperator(
    task_id="ingest_data_xls_products",
    python_callable=ingest_data_xls_products,
    dag=dag,
)
xls_supplier = PythonOperator(
    task_id="ingest_data_xls_supplier",
    python_callable=ingest_data_xls_supplier,
    dag=dag,
)
parquet = PythonOperator(
    task_id="ingest_data_parquet_order",
    python_callable=ingest_data_parquet_order,
    dag=dag,
)
avro = PythonOperator(
    task_id="ingest_data_avro_order_items",
    python_callable=ingest_data_avro_order_items,
    dag=dag,
)
# Define the DummyOperator tasks
fisrt_task = DummyOperator(task_id='first_task')
last_task = DummyOperator(task_id='last_task')

# Define the task dependencies
fisrt_task >> csv >> last_task
fisrt_task >> json_login_attempts >> last_task
fisrt_task >> json_coupons >> last_task
fisrt_task >> xls_products_category >> last_task
fisrt_task >> xls_product >> last_task
fisrt_task >> xls_supplier >> last_task
fisrt_task >> parquet >> last_task
fisrt_task >> avro >> last_task

