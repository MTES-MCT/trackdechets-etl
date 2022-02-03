from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import os
from os.path import join
import pandas as pd


# DAG dedicated to the loading of a subset of company data to data.gouv.fr

@task()
def init_dir() -> str:
    import os

    tmp_data_dir: str = join(Variable.get("TMP_DATA_DIR_BASE"), 'tmp' + str(datetime.now()), '')
    Variable.set('TMP_DATA_DIR', tmp_data_dir)

    os.mkdir(tmp_data_dir)
    return tmp_data_dir


@task()
def query_database(tmp_data_dir) -> str:
    from sqlalchemy import create_engine

    connection = create_engine(Variable.get('DATABASE_URL'))
    df = pd.read_sql_query("""
    SELECT "Company"."siret", "Company"."createdAt", "Company"."name", "Company"."verificationStatus"
    FROM "default$default"."Company"
    """, con=connection, dtype={'siret': str})

    company_pickle_path = join(tmp_data_dir, 'company.pkl')
    df.to_pickle(company_pickle_path)

    return company_pickle_path


@task()
def filter_company_data(company_pickle_path) -> str:
    tmp_data_dir = Variable.get('TMP_DATA_DIR')

    df = pd.read_pickle(company_pickle_path)
    df = df.loc[df['verificationStatus'] == 'VERIFIED']

    company_filtered_pickle_path = join(tmp_data_dir, 'company_filtered.pkl')
    df.to_pickle(company_filtered_pickle_path)

    return company_filtered_pickle_path


@task()
def join_non_diffusible(company_filtered_pickle_path) -> str:
    from sqlalchemy import create_engine

    tmp_data_dir = Variable.get('TMP_DATA_DIR')

    connection = create_engine(Variable.get('DATABASE_URL'))
    df: pd.DataFrame = pd.read_sql_query("""
    SELECT "AnonymousCompany"."siret"
    FROM "default$default"."AnonymousCompany"
    """, con=connection, dtype={'siret': str})
    df.to_csv(join(tmp_data_dir, 'anonymous.csv'))

    df['non_diffusible'] = 'oui'
    company_filtered = pd.read_pickle(company_filtered_pickle_path)
    company_filtered_anonymous = company_filtered.merge(right=df, on='siret', how='left')

    return "True"


# @task()
# def load_backup(tar_file) -> str:
#     # The bash script can probably get the env var too
#     # database_url = os.getenv("SCALINGO_DATABASE_URL")
#     subprocess.call(['bash/load_tar_to_db.sh', tar_file])


@dag(start_date=datetime(2021, 1, 1),
     schedule_interval=None,
     user_defined_macros={},
     catchup=False)
def load_data_dag():
    tmp_data_dir = init_dir()
    company_pickle_path = query_database(tmp_data_dir)
    company_filtered_pickle_path = filter_company_data(company_pickle_path)
    join_non_diffusible(company_filtered_pickle_path)


publish_open_data_etl = load_data_dag()
