FROM python:3.4

# Install all debian packages
RUN apt-get update && apt-get install -y \
	gcc \
	python-dev \
	mysql-client libmysqlclient-dev \
	postgresql-client libpq-dev \
	sqlite3 \
	vim net-tools \
	--no-install-recommends && rm -rf /var/lib/apt/lists/*

# Install all python dependency libs
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

# Copy all source files to the container's working directory
COPY . /app/
WORKDIR /app/

# Create the log directory for uwsgi and the shared directory for static files
RUN mkdir /var/log/uwsgi /shared

# Collect django static files and then copy them to the shared volume directory
RUN python files_django/manage.py collectstatic --noinput &&\
	cp -r files_django/static_root /shared/

VOLUME /shared

# Exposed port for communication with NGINX
EXPOSE 29000

# Start uWSGI on container startup
CMD /usr/local/bin/uwsgi --emperor files_uwsgi/sites --gid www-data --logto /var/log/uwsgi/emperor.log
