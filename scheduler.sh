#!/usr/bin/bash

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
echo "Wait for init..."
sleep 5
echo "Starting the scheduler..."
airflow scheduler