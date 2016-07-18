FROM nginx:1.11

# Install all debian packages
RUN apt-get update && apt-get install -y \
	vim \
	net-tools \
	--no-install-recommends && rm -rf /var/lib/apt/lists/*

# Copy all source files to the container's working directory
COPY . /app/
WORKDIR /app/

# Use the configuration script as the entrypoint
COPY entrypoint.sh /usr/local/bin/
ENTRYPOINT ["entrypoint.sh"]
CMD ["nginx", "-g", "daemon off;"]
