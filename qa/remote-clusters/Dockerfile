FROM haproxy:2.1.2

# Install netcat
RUN apt-get update && \
    apt-get install -y netcat-traditional && \
    rm -rf /var/lib/apt/lists/*
