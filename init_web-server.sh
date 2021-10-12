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

if [[ ! $SCALINGO_POSTGRESQL_URL ]]
  then
  ln -s `pwd`/dags $AIRFLOW_HOME/dags
fi

pwd
ls -l

# start the web server, default port is 8080
echo "Starting the webserver..."
port=${PORT:-8080}
airflow webserver --port $port &

./scheduler.sh
# visit localhost:8080 in the browser and use the admin account you just
# created to login.