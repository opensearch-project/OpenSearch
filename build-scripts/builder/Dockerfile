# Use the Ubuntu jammy image from AWS ECR Public (no pull-rate limits on CodeBuild).
FROM public.ecr.aws/ubuntu/ubuntu:jammy

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

# Architecture-aware JDK 21 from Adoptium/Temurin, which ships both x64 and
# aarch64 Linux builds (the OpenJDK RI is x64-only). TARGETARCH from Buildx,
# else the host arch. --strip-components keeps JAVA_HOME stable at /opt/jdk-21.
ARG TARGETARCH
RUN set -eux; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "$arch" in \
      amd64) jdk_arch="x64" ;; \
      arm64) jdk_arch="aarch64" ;; \
      *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
    esac; \
    mkdir -p /opt/jdk-21; \
    wget -O- "https://api.adoptium.net/v3/binary/latest/21/ga/linux/${jdk_arch}/jdk/hotspot/normal/eclipse" \
      | tar xz -C /opt/jdk-21 --strip-components=1

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/opt/jdk-21
ENV PATH=$JAVA_HOME/bin:$PATH
ENV GRADLE_VERSION=8.14
ENV GRADLE_OPTS="-Xmx2048m -XX:ReservedCodeCacheSize=440m -Dmaven.repo.local=/var/m2"
ENV GRADLE_USER_HOME=/var/gradle

# Download gradle
RUN wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -P /tmp \
    && unzip -d /opt/gradle /tmp/gradle-${GRADLE_VERSION}-bin.zip \
    && rm /tmp/gradle-${GRADLE_VERSION}-bin.zip

# Set GRADLE_HOME environment variable
ENV GRADLE_HOME=/opt/gradle/gradle-${GRADLE_VERSION}
ENV PATH=$GRADLE_HOME/bin:$PATH

# Clean up APT when done
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Create a non-root user
RUN addgroup --gid 1000 wazuh-indexer && \
    adduser --ingroup wazuh-indexer --uid 1000 wazuh-indexer

# Gradle/Maven caches outside the bind-mounted workspace, owned by UID 1000.
RUN mkdir -p /var/gradle /var/m2 && \
    chown wazuh-indexer:wazuh-indexer /var/gradle /var/m2

# Create repositories path
RUN mkdir -p /repositories/wazuh-indexer-plugins && \
    mkdir -p /repositories/wazuh-indexer-reporting && \
    mkdir -p /repositories/wazuh-indexer-security-analytics && \
    mkdir -p /repositories/wazuh-indexer-common-utils && \
    mkdir -p /repositories/wazuh-indexer-notifications && \
    mkdir -p /repositories/wazuh-indexer-alerting && \
    chown wazuh-indexer:wazuh-indexer /repositories/wazuh-indexer-plugins && \
    chown wazuh-indexer:wazuh-indexer /repositories/wazuh-indexer-reporting && \
    chown wazuh-indexer:wazuh-indexer /repositories/wazuh-indexer-security-analytics && \
    chown wazuh-indexer:wazuh-indexer /repositories/wazuh-indexer-common-utils && \
    chown wazuh-indexer:wazuh-indexer /repositories/wazuh-indexer-notifications && \
    chown wazuh-indexer:wazuh-indexer /repositories/wazuh-indexer-alerting

RUN git config --system --add safe.directory '*'

# Copy your build scripts into the container as root
COPY build-scripts/builder/entrypoint.sh /

# Change file permissions as root
RUN chmod +x /entrypoint.sh

# Switch to non-root user
USER wazuh-indexer

# Set the working directory
WORKDIR /home/wazuh-indexer

# Entry point to the build script
ENTRYPOINT ["/entrypoint.sh"]
