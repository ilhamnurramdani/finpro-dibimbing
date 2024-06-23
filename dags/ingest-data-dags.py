import pandas as pd 
from datetime import datetime
import os
import glob
import fastavro
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from sqlalchemy import create_engine


# Fungsi untuk membuat koneksi ke database
def get_conn():
    connection_string = 'postgresql://final_db_owner:m2Y1DOvsSxKR@ep-divine-grass-a5m7v7e4.us-east-2.aws.neon.tech/final_db?sslmode=require'   
    engine = create_engine(connection_string)
    return engine.connect()

# Fungsi untuk memasukkan data dari file CSV
def ingest_data_csv():
    conn = get_conn()
    files = glob.glob('data/customer_*.csv')
    for file in files:
        df = pd.read_csv(file)
        df.to_sql('customers', conn, if_exists='append', index=False)
    conn.close()

# Fungsi untuk memasukkan data dari file JSON (login attempts)
def ingest_data_json_login_attempts():
    conn = get_conn()
    files = glob.glob('data/login_attempts_*.json')
    for file in files :
        df = pd.read_json(file)
        df.to_sql('login_attempts', conn, if_exists='append', index=False)
    conn.close()

# Fungsi untuk memasukkan data dari file JSON (coupons)
def ingest_data_json_coupons():
    conn = get_conn()
    files = glob.glob('data/coupons.json')
    for file in files :
        df = pd.read_json(file)
        df.to_sql('coupons', conn, if_exists='append', index=False)
    conn.close()

# Fungsi untuk memasukkan data dari file Excel (products)
def ingest_data_xls_products():
    conn = get_conn()
    files = glob.glob('data/product.xls')
    for file in files :
        df = pd.read_excel(file)
        df.to_sql('products', conn, if_exists='append', index=False)
    conn.close()

# Fungsi untuk memasukkan data dari file Excel (product category)
def ingest_data_xls_product_category():
    conn = get_conn()
    files = glob.glob('data/product_category.xls')
    for file in files :
        df = pd.read_excel(file)
        df.to_sql('product_category', conn, if_exists='append', index=False)
    conn.close()

# Fungsi untuk memasukkan data dari file Excel (supplier)
def ingest_data_xls_supplier():
    conn = get_conn()
    files = glob.glob('data/supplier.xls')
    for file in files :
        df = pd.read_excel(file)
        df.to_sql('supplier', conn, if_exists='append', index=False)
    conn.close()

# Fungsi untuk memasukkan data dari file Parquet (orders)
def ingest_data_parquet_order(): 
    conn = get_conn()
    files = glob.glob('data/order.parquet')
    for file in files :
        df = pd.read_parquet(file)
        df.to_sql('orders', conn, if_exists='append', index=False)
    conn.close()

# Fungsi untuk memasukkan data dari file Avro (order items)
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

# Argumen default untuk DAG
default_args = {
    "owner" : "Kelompok 3",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Definisi DAG
dag = DAG(
    "ingest_data",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
)

# Definisi tugas-tugas PythonOperator
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

# Definisi tugas DummyOperator sebagai tugas awal dan akhir
first_task = DummyOperator(task_id='first_task')
last_task = DummyOperator(task_id='last_task')

# Menentukan urutan eksekusi tugas dalam DAG
first_task >> csv >> last_task
first_task >> json_login_attempts >> last_task
first_task >> json_coupons >> last_task
first_task >> xls_products_category >> last_task
first_task >> xls_product >> last_task
first_task >> xls_supplier >> last_task
first_task >> parquet >> last_task
first_task >> avro >> last_task
