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

# initialize the database
airflow db init

airflow users create \
    --username $AIRFLOW_USERNAME \
    --firstname $AIRFLOW_FIRSTNAME \
    --lastname $AIRFLOW_LASTNAME \
    --role Admin \
    --email $AIRFLOW_EMAIL \
    --password $AIRFLOW_PASSWORD

ln -s `pwd`/dags $AIRFLOW_HOME/dags

# Install scalingo CLI for secure remote DB access
# curl -O https://cli-dl.scalingo.io/install && bash install

# Login Scalingo
# scalingo login --api-token $SCALINGO_API_TOKEN



# start the web server, default port is 8080
echo "Starting the webserver..."
port=${PORT:-8080}
airflow webserver --port $port --daemon &

# If deployed on Scalingo, start the scheduler here
# otherwise, run the scheduler command separately, once the webserver is up
if [[ $SCALINGO_POSTGRESQL_URL ]]
then
  airflow scheduler
fi

# visit localhost:8080 in the browser and use the admin account you just
# created to login.