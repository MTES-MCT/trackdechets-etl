#!/usr/bin/env bash

# Configure this according to your environment
# Then save it as env.sh

# Airflow home dir
# /!\ this directory is erased at the beginning of init_airflow.sh
# Save your DAGs in ./dags, they are copied automatically
export AIRFLOW_HOME=~/airflow

# Airflow user
export AIRFLOW_USERNAME=rogerrabbit
export AIRFLOW_FIRSTNAME=Roger
export AIRFLOW_LASTNAME=Rabbit
export AIRFLOW_EMAIL=rogerrabbit@acme.com
export AIRFLOW_PASSWORD=iloveyoujessica

DATA_ROOT_DIR="/media/user/data"

export AIRFLOW_VAR_ICPE_URL="https://example.georisques.fr/s3ic.tar.gz"
export AIRFLOW_VAR_GEREP_PATH="${DATA_ROOT_DIR}/georisques-gerep/gerep.csv"
export AIRFLOW_VAR_IREP_PATH="${DATA_ROOT_DIR}/georisques-irep/etablissements.csv"
export AIRFLOW_VAR_TMP_DATA_DIR_BASE="/home/user/data/tmp"

# Airflow settings override
export AIRFLOW__CORE__LOAD_EXAMPLES=False
