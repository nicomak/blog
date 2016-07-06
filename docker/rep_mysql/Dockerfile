FROM mysql:5.7

# Install all debian packages
RUN apt-get update && apt-get install -y \
	openssh-client \
	vim \
	net-tools \
	--no-install-recommends && rm -rf /var/lib/apt/lists/*

# Copy all source files to the container's working directory
COPY . /app/
WORKDIR /app/

# Default env variables for entrypoint script
ENV SERVER_ID 0
ENV BIND_ADDRESS 0.0.0.0 

COPY new-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["new-entrypoint.sh"]
CMD ["mysqld"]
