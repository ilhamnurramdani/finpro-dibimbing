from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

def get_conn():
    connection_string = "postgresql://postgres:postgres@postgres-db:5432/postgres-dw"
    engine = create_engine(connection_string)
    return engine.connect()

def get_conn_dw():
    connection_string = "postgresql://postgres:postgres@postgres-db:5432/postgres-dm"
    engine = create_engine(connection_string)
    return engine.connect()

def burn_rate_analisis():
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT
        DATE_TRUNC('day', s.order_date) AS day,
        SUM(p.price * s.amount * COALESCE(c.discount_percent, 0) / 100) AS total_discount_given
    FROM fact_sales s
    JOIN dim_products p ON s.product_id = p.id
    LEFT JOIN dim_couponss c ON s.coupon_id = c.id
    GROUP BY day
    ORDER BY day
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
        
    create_table_query = """
    CREATE TABLE IF NOT EXISTS burn_rate_analisis(
            day TIMESTAMP,
            total_discount_given INT
        );
        """
    target_conn.execute(create_table_query)

    insert_query = """
        INSERT INTO burn_rate_analisis(day, total_discount_given)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
        """

    with target_conn.begin():
        with target_conn.connection.cursor() as cursor:
            cursor.executemany(insert_query, rows_tuples)

def login_purchase_correlations():
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT 
        CONCAT(c.first_name, ' ', c.last_name) AS full_name,
        MIN(la.attempted_at) AS first_login,
        MAX(la.attempted_at) AS last_login,
        COUNT(DISTINCT la.id) AS login_count,
        COUNT(DISTINCT fs.order_id) AS total_orders,
        SUM(fs.amount) AS total_items_bought
    FROM 
        dim_customers c
    JOIN 
        fact_login_attempts la ON c.id = la.customer_id
    LEFT JOIN 
        fact_sales fs ON c.id = fs.customer_id
    GROUP BY 
        c.first_name, c.last_name
    ORDER BY 
        login_count DESC;
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
        
    create_table_query = """
    CREATE TABLE IF NOT EXISTS login_purchase_correlations(
            customer_name VARCHAR(50),
            first_login TIMESTAMP,
            last_login TIMESTAMP,
            login_count INT,
            total_orders INT,
            total_items_bought INT
        );
        """
    target_conn.execute(create_table_query)

    insert_query = """
        INSERT INTO login_purchase_correlations(customer_name, first_login, last_login, login_count, total_orders, total_items_bought)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """

    with target_conn.begin():
        with target_conn.connection.cursor() as cursor:
            cursor.executemany(insert_query, rows_tuples)

def RFM_analisis():
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT
        CONCAT(c.first_name, ' ', c.last_name) AS full_name,
        MAX(fs.order_date) AS last_purchase_date,
        COUNT(fs.order_id) AS frequency,
        SUM(fs.total_amount) AS monetary
    FROM
        dim_customers c
    JOIN
        fact_sales fs ON c.id = fs.customer_id
    GROUP BY
        c.first_name, c.last_name
    ORDER BY
        last_purchase_date DESC, frequency DESC, monetary DESC;
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
        
    create_table_query = """
    CREATE TABLE IF NOT EXISTS RFM_analisis(
            customer_name VARCHAR(50),
            last_purchase_date TIMESTAMP,
            frequency INT,
            monetary INT
        );
        """
    target_conn.execute(create_table_query)

    insert_query = """
        INSERT INTO RFM_analisis(customer_name, last_purchase_date, frequency, monetary)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """

    with target_conn.begin():
        with target_conn.connection.cursor() as cursor:
            cursor.executemany(insert_query, rows_tuples)

def top_spender():
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT
        concat(c.first_name,' ', c.last_name) as Customer_Name,
        SUM(fs.total_price) AS total_spending
    FROM fact_sales fs
    JOIN dim_customers c ON fs.customer_id = c.id
    GROUP BY c.first_name, c.last_name
    ORDER BY total_spending DESC
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
        
    create_table_query = """
    CREATE TABLE IF NOT EXISTS top_spender(
            customer_name VARCHAR(50),
            total_spending INT
        );
        """
    target_conn.execute(create_table_query)

    insert_query = """
        INSERT INTO top_spender(customer_name, total_spending)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
        """

    with target_conn.begin():
        with target_conn.connection.cursor() as cursor:
            cursor.executemany(insert_query, rows_tuples)

def product_sales_category() :
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT
        p.name AS product_name,
        cp.name AS category_name,
        SUM(fs.total_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_products p ON fs.product_id = p.id
    JOIN dim_category cp ON p.category_id = cp.id
    GROUP BY p.name, cp.name
    ORDER BY total_sales DESC;
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
        
    create_table_query = """
    CREATE TABLE IF NOT EXISTS product_sales_category(
            product_name VARCHAR(50),
            category_name VARCHAR(50),
            total_sales INT
        );
        """
    target_conn.execute(create_table_query)

    insert_query = """
        INSERT INTO product_sales_category(product_name, category_name, total_sales)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
        """

    with target_conn.begin():
        with target_conn.connection.cursor() as cursor:
            cursor.executemany(insert_query, rows_tuples)

def customer_gender():
    source_conn = get_conn()
    target_conn = get_conn_dw()

    query = """
    SELECT
        gender,
        COUNT(*) AS customer_count
    FROM dim_customers c
    join fact_sales fs on c.id = fs.customer_id
    GROUP BY gender
    """
    result = source_conn.execute(query)
    rows = result.fetchall()
    rows_tuples = [tuple(row) for row in rows]
        
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ustomer_gender(
            gender VARCHAR(50),
            customer_count INT
        );
        """
    target_conn.execute(create_table_query)

    insert_query = """
        INSERT INTO ustomer_gender(gender, customer_count)
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
    "data-marts",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2023, 6, 1),
    catchup=False,
)

burn_rate_analisis_task = PythonOperator(
    task_id = "burn_rate_analisis_task",
    python_callable = burn_rate_analisis,
    dag=dag
)

login_purchase_correlations_task = PythonOperator(
    task_id = "login_purchase_correlations_task",
    python_callable = login_purchase_correlations,
    dag=dag
)

RFM_analisis_task = PythonOperator(
    task_id = "RFM_analisis_task",
    python_callable = RFM_analisis,
    dag=dag
)

top_spender_task = PythonOperator(
    task_id = "top_spender_task",
    python_callable = top_spender,
    dag=dag
)

product_sales_category_task = PythonOperator(
    task_id = "product_sales_category_task",
    python_callable = product_sales_category,
    dag=dag
)

customer_gender_task = PythonOperator(
    task_id = "customer_gender_task",
    python_callable = customer_gender,
    dag=dag
)

first_task = DummyOperator(task_id='first_task')
last_task = DummyOperator(task_id='last_task')

first_task >> burn_rate_analisis_task >> last_task
first_task >> login_purchase_correlations_task >> last_task
first_task >> RFM_analisis_task >> last_task
first_task >> top_spender_task>> last_task
first_task >> product_sales_category_task >> last_task
first_task >> customer_gender_task >> last_task