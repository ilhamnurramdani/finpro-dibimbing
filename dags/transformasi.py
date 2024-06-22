from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

def get_conn():
    connection_string = "postgresql://postgres:postgres@postgres-db:5432/postgres"
    engine = create_engine(connection_string)
    return engine.connect()

def get_conn_dw():
    connection_string = "postgresql://postgres:postgres@postgres-db:5432/postgres-dw"
    engine = create_engine(connection_string)
    return engine.connect()

def fact_sales():
    # Koneksi ke database Neon
    source_conn = get_conn()
        
    # Koneksi ke database Data Warehouse
    target_conn = get_conn_dw()
    query = """ 
    SELECT
            o.id AS order_id,
            o.customer_id,
            oi.product_id,
            oi.coupon_id,
            o.status AS order_status,
            o.created_at AS order_date,
            oi.amount,
            p.price,
            (oi.amount * p.price) AS total_amount,
            COALESCE(c.discount_percent, 0) AS discount_percent,
            (oi.amount * p.price) - (CASE WHEN c.id IS NOT NULL THEN (oi.amount * p.price * c.discount_percent / 100) ELSE 0 END) AS total_price
        FROM orders o
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        LEFT JOIN coupons c ON oi.coupon_id = c.id;
    """
    # Eksekusi query dan ambil hasilnya
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
    create_table_query = """
    CREATE TABLE IF NOT EXISTS fact_sales(
            order_id INT PRIMARY KEY,
            customer_id INT,
            product_id INT,
            coupon_id INT,
            order_status VARCHAR(50),
            order_date DATE,
            amount INT,
            price INT,
            total_amount INT,
            discount_percent INT,
            total_price INT
        );
    """
    target_conn.execute(create_table_query)
    insert_query = """
         INSERT INTO fact_sales (
            order_id, customer_id, product_id, coupon_id, order_status, order_date, amount, price, total_amount, discount_percent, total_price
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
    with target_conn.begin():
        with target_conn.connection.cursor() as cursor:
            cursor.executemany(insert_query, rows_tuples)

def fact_login_attempts():
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT 
            id,
            customer_id,
            login_successful,
            attempted_at
        FROM login_attempts;
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
    create_table_query = """
    CREATE TABLE IF NOT EXISTS fact_login_attempts(
        id INT PRIMARY KEY,
        customer_id INT,
        login_successful BOOLEAN,
        attempted_at TIMESTAMP
    );
    """
    target_conn.execute(create_table_query)
    insert_query = """
    INSERT INTO fact_login_attempts(
        id, customer_id, login_successful, attempted_at
    ) VALUES(%s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    with target_conn.begin() :
        with target_conn.connection.cursor() as cursor :
            cursor.executemany(insert_query, rows_tuples)

def dim_customers() :
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT 
            id,
            first_name,
            last_name,
            gender,
            address,
            zip_code
        FROM customers;
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
    create_table_query ="""
    CREATE TABLE IF NOT EXISTS dim_customers(
        id INT PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender CHAR(1),
        address VARCHAR(100),
        zip_code VARCHAR(10)
    );
    """
    target_conn.execute(create_table_query)
    insert_query = """
        INSERT INTO dim_customers(
            id,first_name,last_name,gender,address,zip_code
        )VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    with target_conn.begin() :
        with target_conn.connection.cursor() as cursor :
            cursor.executemany(insert_query, rows_tuples)

def dim_orders() :
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT 
            id,
            customer_id,
            status,
            created_at
        FROM orders;
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
    create_table_query = """
        CREATE TABLE IF NOT EXISTS dim_orders(
            id INT PRIMARY KEY,
            customer_id int, 
            status VARCHAR(20),
            created_at DATE
        );
    """
    target_conn.execute(create_table_query)
    insert_query = """
        INSERT INTO dim_orders(
            id, customer_id, status, created_at
        ) VALUES(%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    with target_conn.begin() :
        with target_conn.connection.cursor() as cursor :
            cursor.executemany(insert_query, rows_tuples)

def dim_coupons():
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT 
            id,
            discount_percent
        FROM coupons;
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
        
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dim_couponss(
            id INT PRIMARY KEY,
            discount_percent INT
        );
        """
    target_conn.execute(create_table_query)

    insert_query = """
        INSERT INTO dim_couponss(id, discount_percent)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
        """

    with target_conn.begin():
        with target_conn.connection.cursor() as cursor:
            cursor.executemany(insert_query, rows_tuples)


def dim_products():
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT 
            id,
            name,
            price,
            category_id,
            supplier_id 
        FROM products;
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dim_products(
        id INT PRIMARY KEY,
        name VARCHAR(30),
        price INT,
        category_id INT,
        supplier_id INT
    );
    """
    target_conn.execute(create_table_query)
    insert_query = """
    INSERT INTO dim_products(
        id, name, price, category_id, supplier_id
    ) VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    with target_conn.begin() :
        with target_conn.connection.cursor() as cursor :
            cursor.executemany(insert_query, rows_tuples)

def dim_order_items():
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT 
            id,
            order_id,
            product_id,
            amount,
            coupon_id
        FROM order_items;
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dim_order_items(
        id INT PRIMARY KEY,
        order_id INT,
        product_id INT,
        amount INT,
        coupon_id INT
    );
    """
    target_conn.execute(create_table_query)
    insert_query = """
    INSERT INTO dim_order_items(
        id, order_id, product_id, amount, coupon_id
    ) VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    with target_conn.begin() :
        with target_conn.connection.cursor() as cursor :
            cursor.executemany(insert_query, rows_tuples)

def dim_category():
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT 
            id,
            name
        FROM product_category;
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
        
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dim_category(
            id INT PRIMARY KEY,
            name VARCHAR(50)
        );
        """
    target_conn.execute(create_table_query)

    insert_query = """
        INSERT INTO dim_category(id, name)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
        """

    with target_conn.begin():
        with target_conn.connection.cursor() as cursor:
            cursor.executemany(insert_query, rows_tuples)

default_args = {
    "owner" : "Kelompok 3",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "transformasi",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2023, 6, 1),
    catchup=False,
)

fact_sales_task = PythonOperator(
    task_id="fact_sales_task",
    python_callable= fact_sales,
    dag = dag,
)

fact_login_attempts_task = PythonOperator(
    task_id="fact_login_attempts_task",
    python_callable=fact_login_attempts,
    dag=dag,
)

dim_customers_task = PythonOperator(
    task_id="dim_customers_task",
    python_callable=dim_customers,
    dag=dag,
)

dim_orders_task = PythonOperator(
    task_id="dim_orders_task",
    python_callable=dim_orders,
    dag=dag,
)

dim_coupons_task = PythonOperator(
    task_id="dim_coupons_task",
    python_callable=dim_coupons,
    dag=dag,
)

dim_products_task = PythonOperator(
    task_id="dim_products_task",
    python_callable=dim_products,
    dag=dag,
)

dim_order_items_task = PythonOperator(
    task_id="dim_order_items_task",
    python_callable=dim_order_items,
    dag=dag,
)

dim_category_task = PythonOperator(
    task_id = 'dim_category_task',
    python_callable = dim_category,
    dag = dag
)

first_task = DummyOperator(task_id='first_task')
last_task = DummyOperator(task_id='last_task')

first_task >> fact_sales_task >> last_task
first_task >> fact_login_attempts_task >> last_task
first_task >> dim_customers_task >> last_task
first_task >> dim_orders_task >> last_task
first_task >> dim_coupons_task >> last_task
first_task >> dim_products_task >> last_task
first_task >> dim_order_items_task >> last_task
first_task >> dim_category_task >> last_task