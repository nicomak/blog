FROM python:3.4-slim

# Install all debian packages
RUN apt-get update && apt-get install -y \
		gcc \
		mysql-client libmysqlclient-dev \
		postgresql-client libpq-dev \
		sqlite3 \
		vim net-tools \
		supervisor \
		nginx \
	--no-install-recommends && rm -rf /var/lib/apt/lists/*

# Install all python dependency libs
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

# Copy all source files to the container's working directory
COPY . /app/
WORKDIR /app/

# Configure Nginx, uWSGI and supervisord
RUN ( cp nginx.conf /etc/nginx/nginx.conf &&\
	mkdir /var/log/uwsgi &&\
	ln -s supervisord.conf /etc/supervisor/conf.d/)

# Collect django static files
RUN python files_django/manage.py collectstatic --noinput

# Exposed port
EXPOSE 80

CMD ["/usr/bin/supervisord"]
