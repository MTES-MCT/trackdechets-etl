#!/usr/bin/env bash

# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)

# Set via env file
# export AIRFLOW_HOME=~/airflow

source ./env.sh

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

cp -r dags $AIRFLOW_HOME/dags

# start the web server, default port is 8080
echo "Starting the webserver..."
airflow webserver --port 8080 &> webserver.log &

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
echo "Starting the scheduler..."
airflow scheduler

# visit localhost:8080 in the browser and use the admin account you just
# created to login.