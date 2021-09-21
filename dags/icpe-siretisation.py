from airflow import DAG
from datetime import datetime

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

def helloWorld():
    print("Hi guys!")

def imHere():
    print("I'm heeeere!")


with DAG(dag_id="icpe-siretisation",
         start_date=datetime(2021,1,1),
         schedule_interval="@daily",
         catchup=False) as dag:

    hello_world = PythonOperator(
        task_id = "hello_world",
        python_callable = helloWorld)

    im_here = PythonOperator(
        task_id="im_here",
        python_callable=imHere)

hello_world >> im_here



