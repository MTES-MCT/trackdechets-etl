#!/usr/bin/env bash

# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)

# Set via env file
# export AIRFLOW_HOME=~/airflow

# If not deployed on Scalingo, get the env from env.sh
if [[ ! $SCALINGO_POSTGRESQL_URL ]]
then
  source ./env.sh
fi

# Start from scratch
rm -rf $AIRFLOW_HOME

AIRFLOW_VERSION=2.1.4
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.6
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.1.4/constraints-3.6.txt
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# initialize the database
airflow db init

airflow users create \
    --username $AIRFLOW_USERNAME \
    --firstname $AIRFLOW_FIRSTNAME \
    --lastname $AIRFLOW_LASTNAME \
    --role Admin \
    --email $AIRFLOW_EMAIL \
    --password $AIRFLOW_PASSWORD

if [[ ! $SCALINGO_POSTGRESQL_URL ]]
  ln -s `pwd`/dags $AIRFLOW_HOME/dags
fi

# start the web server, default port is 8080
echo "Starting the webserver..."
port=$PORT || 8080
airflow webserver --port $port

# visit localhost:8080 in the browser and use the admin account you just
# created to login.