from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
import pandas as pd 
import matplotlib.pyplot as plt 


PG_CONN_ID= 'postgres_conn'

default_args= {
    'owner' : 'Kiwilytics',
    'retries' : 1 ,
    'retry_delay' : timedelta(minutes=2)
}

# Extract
def fetch_orders():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn=hook.get_conn()
    query='''
        SELECT o.orderdate::date as order_date ,
        od.orderid,
        p.productname,
        p.price,
        od.quantity 

        FROM orders o 
        JOIN order_details od ON o.orderid = od.orderid
        JOIN  products p ON od.productid = p.productid 
    
    '''
    df = pd.read_sql(query,conn)
    df.to_csv("/home/kiwilytics/airflow_output/daily_sales.csv" ,index=False)

# Transform

def calc_revenue():
    df = pd.read_csv("/home/kiwilytics/airflow_output/daily_sales.csv")
    df['revenue'] = df['price'] * df['quantity']
    daily_revenue = df.groupby('order_date')['revenue'].sum().reset_index()
    daily_revenue.to_csv("/home/kiwilytics/airflow_output/daily_revenue.csv", index=False)

def data_quality():
    df = pd.read_csv("/home/kiwilytics/airflow_output/daily_sales.csv")
    
    # Check for nulls
    if df.isnull().any().any():
        raise ValueError(" Data quality check failed: Null values detected.")
    
    # Check for negative or zero values in quantity/price
    if (df['quantity'] <= 0).any():
        raise ValueError(" Data quality check failed: Non-positive quantities found.")
    if (df['price'] <= 0).any():
        raise ValueError(" Data quality check failed: Non-positive prices found.")
    
    print(f" Data quality check passed: {len(df)} records verified.")

# Plot

def plot_revenue():

    daily_revenue = pd.read_csv("/home/kiwilytics/airflow_output/daily_revenue.csv")
    daily_revenue['order_date'] = pd.to_datetime(daily_revenue['order_date'])

    plt.figure(figsize=(12,6))
    plt.plot(daily_revenue['order_date'], daily_revenue['revenue'], marker='o' , linestyle='-')
    plt.title("Daily Revenue Over Time")
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("/home/kiwilytics/airflow_output/daily_revenue.png")

dag = DAG(
    dag_id="daily_revenue_pipeline",
    default_args=default_args,
    description="Pipeline to calculate daily revenue from Postgres ",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
)


t1 = PythonOperator(
    task_id="fetch_orders",
    python_callable=fetch_orders,
    dag=dag,
)

t2 = PythonOperator(
    task_id="data_quality_check",
    python_callable=data_quality,
    dag=dag,
)

t3 = PythonOperator(
    task_id="calculate_revenue",
    python_callable=calc_revenue,
    dag=dag,
)

t4 = PythonOperator(
    task_id="plot_revenue",
    python_callable=plot_revenue,
    dag=dag,
)

t1 >> t2 >> t3 >> t4

