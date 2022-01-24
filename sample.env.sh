#!/usr/bin/env bash

# Configure this according to your environment
# Then save it as env.sh

# Airflow home dir
# /!\ this directory is erased at the beginning of init_airflow.sh
# Save your DAGs in ./dags
AIRFLOW_HOME=/home/user/airflow
export AIRFLOW_VAR_TMP_DATA_DIR_BASE="$AIRFLOW_HOME"


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

# Postgre tables
export AIRFLOW_VAR_TABLE_INSTALLATIONS="IC_installations"
export AIRFLOW_VAR_TABLE_RUBRIQUES="IC_rubriques"

# Parent directory of the data sources
DATA_ROOT_DIR="/media/colin/data"

# Data sources
export AIRFLOW_VAR_ICPE_URL="${DATA_ROOT_DIR}/georisques-icpe/S3IC-Georisques.tar.gz"

# Airflow settings override
export AIRFLOW__CORE__LOAD_EXAMPLES=False
