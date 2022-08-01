from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json


def captura_conta_dados():
    url = 'https://data.cityofnewyork.us/resource/2npr-yv2b.json'
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd


def verifica_dados(ti):
    qtd = ti.xcom_pull(task_ids='captura_conta_dados')
    if (qtd > 1000):
        return 'valido'
    return 'invalido'


with DAG('tutorial_dag', start_date=datetime(2022, 8, 1), schedule_interval='1 * * * *',
         catchup=False) as dag:

    captura_conta_dados = PythonOperator(
        task_id='captura_conta_dados',
        python_callable=captura_conta_dados
    )

    valido = BashOperator(
        task_id='valido',
        bash_command="echo 'Quantidade OK'"
    )
    invalido = BashOperator(
        task_id='invalido',
        bash_command="echo 'Quantidade invalida'"
    )
    verifica_dados = BranchPythonOperator(
        task_id='verifica_dados',
        python_callable=verifica_dados
    )

    captura_conta_dados >> verifica_dados >> [valido, invalido]
