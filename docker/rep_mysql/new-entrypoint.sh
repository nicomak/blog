#!/bin/bash

# Outputs the contents with replaced environment variables
function replace_env {
	while read line
	do
		if [[ ${line:0:1} == '#' ]]; then
			echo $line
		else
			eval echo "$line"
		fi
	done < $1
}

echo 'Generating /etc/mysql/conf.d/custom.cnf :'
echo '========================================='
replace_env custom.cnf.template | tee /etc/mysql/conf.d/custom.cnf
echo '========================================='

echo 'Generating /app/credentials.cnf :'
echo '========================================='
replace_env credentials.cnf.template | tee credentials.cnf
echo '========================================='

if [[ ! -z "$AUTO_INIT_MASTER_IP" ]]; then
    bash replication-start.sh $AUTO_INIT_MASTER_IP 127.0.0.1 &
fi

# Call the parent image entrypoint
exec /entrypoint.sh "$@"
