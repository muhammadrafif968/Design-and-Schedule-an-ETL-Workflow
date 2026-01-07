import logging
import re

from datetime import timedelta
from airflow.hooks.mysql_hook import MySqlHook
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

# Konfigurasi DAG
# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# Extract functions
# 2.1 Ekstrak Customers
def extract_customers_from_postgres(**context):
    """Ekstrak data customer dari PostgreSQL"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            SELECT *
            FROM raw_data.customers
            WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        cursor.execute(sql)

        cols = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()

        customers = [dict(zip(cols, row)) for row in records]

        context['task_instance'].xcom_push(
            key='customers_data',
            value=customers
        )
#logging panjang customer yang di extract
        logging.info(f"Extracted {len(customers)} customers")

        cursor.close()
        conn.close()
#logging kalau error
    except Exception:
        logging.error("Failed to extract customers", exc_info=True)
        raise

# 2.2 Ekstrak Products
def extract_products_from_postgres(**context):
    """Ekstrak data product dari PostgreSQL"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            SELECT
                p.*,
                s.supplier_name
            FROM raw_data.products p
            JOIN raw_data.suppliers s
              ON p.supplier_id = s.supplier_id
            WHERE p.updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        cursor.execute(sql)

        cols = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()

        products = [dict(zip(cols, row)) for row in records]

        context['task_instance'].xcom_push(
            key='products_data',
            value=products
        )
#logging panjang product yang di extract
        logging.info(f"Extracted {len(products)} products")

        cursor.close()
        conn.close()
#logging kalau error
    except Exception:
        logging.error("Failed to extract products", exc_info=True)
        raise

# 2.3 Ekstrak Orders
def extract_orders_from_postgres(**context):
    """Ekstrak data order dari PostgreSQL"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            SELECT *
            FROM raw_data.orders
            WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        cursor.execute(sql)

        cols = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()

        orders = [dict(zip(cols, row)) for row in records]

        context['task_instance'].xcom_push(
            key='orders_data',
            value=orders
        )
# logging report panjang orders yang di extract
        logging.info(f"Extracted {len(orders)} orders")

        cursor.close()
        conn.close()
#logging kalau error
    except Exception:
        logging.error("Failed to extract orders", exc_info=True)
        raise
# 3.1 Transform & Load Customers
def transform_and_load_customers(**context):
    """Transform data customer dan load ke MySQL"""
    try:
        ti = context['task_instance']
        customers = ti.xcom_pull(
            task_ids='extract_customers',
            key='customers_data'
        )
# kalau ngak ada data yang di process
        if not customers:
            logging.info("No customer data to process")
            return

        transformed_customers = []

        for customer in customers:
            # Transform phone 
            raw_phone = customer.get('phone', '')
            digits = re.sub(r'\D', '', raw_phone)

            if len(digits) == 10:
                phone = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            else:
                phone = None

            # Transform state
            state = customer.get('state')
            state = state.upper() if state else None

            transformed_customers.append((
                customer.get('customer_id'),
                phone,
                state
            ))

        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO dim_customers (customer_id, phone, state)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                phone = VALUES(phone),
                state = VALUES(state)
        """

        cursor.executemany(sql, transformed_customers)
        conn.commit()
# logging jumlah customer yang udah di transform
        logging.info(f"Upserted {len(transformed_customers)} customers into dim_customers")

        cursor.close()
        conn.close()
# logging kalau error
    except Exception:
        logging.error("Failed to transform and load customers", exc_info=True)
        raise
# 3.2 Transform & Load Products
def transform_and_load_products(**context):
    """Transform data product dan load ke MySQL"""
    try:
        ti = context['task_instance']
        products = ti.xcom_pull(
            task_ids='extract_products',
            key='products_data'
        )
# kalau ngak ada data yang di process
        if not products:
            logging.info("No product data to process")
            return

        transformed_products = []

        for product in products:
            price = product.get('price', 0) or 0
            cost = product.get('cost', 0) or 0

            # Transform margin
            if price > 0:
                margin = ((price - cost) / price) * 100
            else:
                margin = 0

            # Transform category
            category = product.get('category')
            category = category.title() if category else None

            transformed_products.append((
                product.get('product_id'),
                product.get('product_name'),
                category,
                product.get('supplier_name'),
                price,
                cost,
                round(margin, 2)
            ))

        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO dim_products (
                product_id,
                product_name,
                category,
                supplier_name,
                price,
                cost,
                margin
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                category = VALUES(category),
                supplier_name = VALUES(supplier_name),
                price = VALUES(price),
                cost = VALUES(cost),
                margin = VALUES(margin)
        """

        cursor.executemany(sql, transformed_products)
        conn.commit()
#logging prududct yang berhasil di transform
        logging.info(f"Upserted {len(transformed_products)} products into dim_products")

        cursor.close()
        conn.close()
#logging kalau gagal
    except Exception:
        logging.error("Failed to transform and load products", exc_info=True)
        raise
# 3.3 Transform & Load Orders
def transform_and_load_orders(**context):
    """Transform data order dan load ke MySQL"""
    try:
        ti = context['task_instance']
        orders = ti.xcom_pull(
            task_ids='extract_orders',
            key='orders_data'
        )
#kalau ngak ada data yang diproses
        if not orders:
            logging.info("No order data to process")
            return

        transformed_orders = []

        for order in orders:
            # Transform status 
            status = order.get('status')
            status = status.lower() if status else None

            # Validate total_amount 
            total_amount = order.get('total_amount', 0) or 0
            if total_amount < 0:
                logging.warning(
                    f"Negative total_amount for order_id {order.get('order_id')}, set to 0"
                )
                total_amount = 0

            transformed_orders.append((
                order.get('order_id'),
                order.get('customer_id'),
                status,
                total_amount,
                order.get('order_date')
            ))

        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO fact_orders (
                order_id,
                customer_id,
                order_status,
                total_amount,
                order_date
            )
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                order_status = VALUES(order_status),
                total_amount = VALUES(total_amount),
                order_date = VALUES(order_date)
        """

        cursor.executemany(sql, transformed_orders)
        conn.commit()
# logging info jumlah order yang udah di transformasi
        logging.info(f"Upserted {len(transformed_orders)} orders into fact_orders")

        cursor.close()
        conn.close()
# longging jikalau gagal
    except Exception:
        logging.error("Failed to transform and load orders", exc_info=True)
        raise

# DAG definition
with DAG(
    dag_id='postgres_to_mysql_etl',
    default_args=default_args,
    schedule_interval=timedelta(hours=6),
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'postgresql', 'mysql', 'data-pipeline'],
) as dag:

    extract_customers = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers_from_postgres,
    )

    extract_products = PythonOperator(
        task_id='extract_products',
        python_callable=extract_products_from_postgres,
    )

    extract_orders = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders_from_postgres,
    )

    load_customers = PythonOperator(
        task_id='transform_and_load_customers',
        python_callable=transform_and_load_customers,
    )

    load_products = PythonOperator(
        task_id='transform_and_load_products',
        python_callable=transform_and_load_products,
    )

    load_orders = PythonOperator(
        task_id='transform_and_load_orders',
        python_callable=transform_and_load_orders,
    )
    # Dependencies
    extract_customers >> load_customers
    extract_products >> load_products
    extract_orders >> load_orders
