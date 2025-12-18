# Use the official Ubuntu Noble image as the base image
FROM ubuntu:jammy

USER root

# Set environment variables for non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive

# Update the package list and install necessary tools
RUN apt-get update && \
    apt-get install -y curl gnupg lsb-release && \
    curl -fsSL https://www.aptly.info/pubkey.txt | gpg --dearmor -o /usr/share/keyrings/aptly.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/aptly.gpg] http://repo.aptly.info/ squeeze main" | tee /etc/apt/sources.list.d/aptly.list && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends \
    wget \
    unzip \
    git \
    build-essential \
    debmake \
    debhelper-compat \
    libxext6 \
    libx11-6 \
    libxi6 \
    libxtst6 \
    libxrender1 \
    aptly \
    cpio \
    rpm \
    rpm2cpio \
    maven \
    jq \
    python3 \
    python3-debian \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install OpenJDK 21
RUN wget -O- https://download.java.net/openjdk/jdk21/ri/openjdk-21+35_linux-x64_bin.tar.gz | tar xz -C /opt/

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/opt/jdk-21
ENV PATH=$JAVA_HOME/bin:$PATH
ENV GRADLE_VERSION=8.14
ENV GRADLE_OPTS="-Xmx2048m -XX:ReservedCodeCacheSize=440m"

# Download gradle
RUN wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -P /tmp \
    && unzip -d /opt/gradle /tmp/gradle-${GRADLE_VERSION}-bin.zip \
    && rm /tmp/gradle-${GRADLE_VERSION}-bin.zip

# Set GRADLE_HOME environment variable
ENV GRADLE_HOME=/opt/gradle/gradle-${GRADLE_VERSION}
ENV PATH=$GRADLE_HOME/bin/$PATH

# Clean up APT when done
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Create a non-root user
RUN addgroup --gid 1000 wazuh-indexer && \
    adduser --ingroup wazuh-indexer --uid 1000 wazuh-indexer

# Create repositories path
RUN mkdir -p /repositories/wazuh-indexer-plugins && \
    mkdir -p /repositories/wazuh-indexer-reporting && \
    mkdir -p /repositories/wazuh-indexer-security-analytics && \
    chown wazuh-indexer:wazuh-indexer /repositories/wazuh-indexer-plugins && \
    chown wazuh-indexer:wazuh-indexer /repositories/wazuh-indexer-reporting && \
    chown wazuh-indexer:wazuh-indexer /repositories/wazuh-indexer-security-analytics

# Copy your build scripts into the container as root
COPY build-scripts/builder/entrypoint.sh /

# Change file permissions as root
RUN chmod +x /entrypoint.sh

# Switch to non-root user
USER wazuh-indexer

# Set the working directory
WORKDIR /home/wazuh-indexer

# Entry point to the build script
ENTRYPOINT ["./entrypoint.sh"]
