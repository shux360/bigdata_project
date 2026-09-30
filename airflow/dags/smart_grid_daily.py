from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.insert(0,"/opt/airflow/project")
from src.producers.tariff_batch import generate_tariffs, publish_file
from src.batch.daily_report import load_tariffs, create_report

DATA_DIR = "/opt/airflow/project/data"

def produce(**ctx):
    path=generate_tariffs(ctx["logical_date"].date(),f"{DATA_DIR}/batch"); publish_file(path); return str(path)
def reconcile(**ctx):
    path=ctx["ti"].xcom_pull(task_ids="generate_and_publish_tariffs"); load_tariffs(path); return str(create_report(ctx["logical_date"].date(),f"{DATA_DIR}/reports"))

with DAG("smart_grid_daily",start_date=datetime(2026,1,1),schedule=timedelta(minutes=5),catchup=False,is_paused_upon_creation=False,tags=["smart-grid","simulated-day"]) as dag:
    generate=PythonOperator(task_id="generate_and_publish_tariffs",python_callable=produce)
    report=PythonOperator(task_id="reconcile_daily_billing",python_callable=reconcile)
    generate >> report
