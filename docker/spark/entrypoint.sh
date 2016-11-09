#!/bin/bash

if [[ "$ROLE" == "master" ]]; then
    bash spark/sbin/start-master.sh
elif [[ "$ROLE" == "slave" && -n "$MASTER_URL" ]]; then
    bash spark/sbin/start-slave.sh "$MASTER_URL"
else
    echo "The ROLE environment variable must be 'master' or 'slave'. For slaves the MASTER_URL must be specified as spark://[master]:7077."
    exit 1
fi

# This tail is necessary. It not only allows to view the logs
# in the standard output, but it also keeps the container alive forever
tail -f spark/logs/*

