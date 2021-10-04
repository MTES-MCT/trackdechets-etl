#!/usr/bin/env bash

# Configure this according to your environment
# Then save it as env.sh

# Airflow home dir
# /!\ this directory is erased at the beginning of init_airflow.sh
# Save your DAGs in ./dags
AIRFLOW_HOME=/home/user/airflow

# Airflow user
AIRFLOW_USERNAME=
AIRFLOW_FIRSTNAME=
AIRFLOW_LASTNAME=
AIRFLOW_EMAIL=
AIRFLOW_PASSWORD=

# Postgre connection
export AIRFLOW_VAR_PGSQL_USER="root"
export AIRFLOW_VAR_PGSQL_PASSWORD=""
export AIRFLOW_VAR_PGSQL_HOST="localhost"
export AIRFLOW_VAR_PGSQL_PORT="5432"
export AIRFLOW_VAR_PGSQL_DATABASE=""
export AIRFLOW_VAR_PGSQL_SCHEMA=""

# Parent directory of the data sources
DATA_ROOT_DIR="/media/colin/data"

# Data sources
export AIRFLOW_VAR_ICPE_URL="${DATA_ROOT_DIR}/georisques-icpe/S3IC-Georisques.tar.gz"
export AIRFLOW_VAR_GEREP_PATH="${DATA_ROOT_DIR}/georisques-gerep/gerep-donnees-2016-2017-fusion.csv"
export AIRFLOW_VAR_IREP_PATH="${DATA_ROOT_DIR}/georisques-irep/etablissements-2019.csv"
export AIRFLOW_VAR_SIRENE_PATH="${DATA_ROOT_DIR}/sirene/StockEtablissement_utf8.csv"
export AIRFLOW_VAR_TMP_DATA_DIR_BASE="/home/colin/git/trackdechets-etl"

# Airflow settings override
export AIRFLOW__CORE__LOAD_EXAMPLES=False
