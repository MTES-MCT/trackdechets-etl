from airflow import DAG
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import requests
import tarfile

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

tmpDataDir = Variable.get("TMP_DATA_DIR")


def getIrepData():
    irepPath = Variable.get("IREP_PATH")
    df = pd.read_csv(
        irepPath,
        usecols=['identifiant', 'numero_siret'],
        sep=';',
        dtype={
            'numero_siret': str,
            'identifiant': str
        })
    df.rename(columns={'identifiant': 's3ic', 'numero_siret': 'siret'})
    df.drop_duplicates(inplace=True)

    print("Longueur dataframe IREP : " + str(len(df)))
    df.to_csv(tmpDataDir + "/irep.csv")


def getGerepData():
    gerepPath = Variable.get("GEREP_PATH")
    df = pd.read_csv(
        gerepPath,
        usecols=['Code établissement', 'Numero Siret'],
        dtype={
            'Code établissement': str,
            'Numero Siret': str
        })
    df.rename(columns={'Code établissement': 's3ic', 'Numero Siret': 'siret'})
    df.drop_duplicates(inplace=True)

    print("Longueur dataframe GEREP : " + str(len(df)))
    df.to_csv(tmpDataDir + "/gerep.csv")


def downloadIcpeData():
    icpeUrl = Variable.get("ICPE_URL")
    icpeData = requests.get(icpeUrl, allow_redirects=True)
    open(tmpDataDir + '/icpe.tar.gz', 'wb').write(icpeData.content)


def getIcpeData():
    icpeTarPath = tmpDataDir + '/icpe.tar.gz'

    # https://stackoverflow.com/a/37474942
    tar = tarfile.open(icpeTarPath, 'r:gz')
    for member in tar.getmembers():
        if member.name == 'IC_etablissement.csv':
            tar.extract(member, path=tmpDataDir + '/icpe_etablissements.ori.csv')


def siretisationIcpe():
    icpePath = tmpDataDir + "/icpe_etablissements.ori.csv"
    return icpePath


with DAG(dag_id="icpe-siretisation",
         start_date=datetime(2021, 1, 1),
         schedule_interval="@daily",
         catchup=True) as dag:
    get_gerep_data = PythonOperator(
        task_id="getGerepData",
        python_callable=getGerepData
    )

    get_irep_data = PythonOperator(
        task_id="getIrepData",
        python_callable=getIrepData
    )

    download_icpe_data = PythonOperator(
        task_id="downloadIcpeData",
        python_callable=downloadIcpeData
    )

    get_icpe_data = PythonOperator(
        task_id="getIcpeData",
        python_callable=getIcpeData
    )

    siretisation_icpe = PythonOperator(
        task_id="siretisationIcpe",
        python_callable=siretisationIcpe
    )

download_icpe_data >> get_icpe_data
[get_gerep_data, get_irep_data, get_icpe_data] >> siretisation_icpe
