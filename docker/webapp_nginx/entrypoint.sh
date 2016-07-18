#!/bin/sh

# Replace the UWSGI uri in the Nginx tcp proxy configuration.
if [ -n "$UWSGI_URI" ]; then
	sed -e "s/{{UWSGI_URI}}/$UWSGI_URI/" nginx.conf.template > /etc/nginx/nginx.conf
else
	echo "ERROR - Must specify: -e UWSGI_URI=<hostname:port>"
	exit 1
fi

exec "$@"
