FROM debian:jessie

# Install all debian packages
RUN apt-get update && apt-get install -y \
		vim net-tools \
		nginx \
	--no-install-recommends && rm -rf /var/lib/apt/lists/*

# Copy all source files to the container's working directory
COPY ./nginx.conf /etc/nginx/

# Exposed port
EXPOSE 80

CMD service nginx start
