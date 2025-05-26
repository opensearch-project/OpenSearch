# Use the official Ubuntu Noble image as the base image
FROM ubuntu:noble

RUN userdel -r ubuntu

# Set environment variables for non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive


# Update the package list and install necessary tools
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y \
    wget \
    curl \
    unzip \
    gnupg2 \
    git \
    build-essential \
    debmake \
    debhelper-compat \
    libxrender1 \
    libxtst6 \
    libxi6 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libatspi2.0-dev \
    libxcomposite-dev \
    libxdamage1 \
    libxfixes3 \
    libxfixes-dev \
    libxrandr2 \
    libgbm-dev \
    libxkbcommon-x11-0 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libcairo2-dev \
    libnss3 \
    libnspr4 \
    libnspr4-dev \
    aptly \
    cpio \
    rpm \
    rpm2cpio \
    maven \
    jq \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install OpenJDK 21
RUN wget -O- https://download.java.net/openjdk/jdk21/ri/openjdk-21+35_linux-x64_bin.tar.gz | tar xz -C /opt/

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/opt/jdk-21
ENV PATH=$JAVA_HOME/bin:$PATH
ENV GRADLE_VERSION=8.10
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
    chown wazuh-indexer:wazuh-indexer /repositories/wazuh-indexer-plugins && \

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
