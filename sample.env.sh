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

export ICPE_URL=
export GEREP_URL=
export IREP_URL=

# Airflow settings override
export AIRFLOW__CORE__LOAD_EXAMPLES=False
