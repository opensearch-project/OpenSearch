FROM ubuntu:jammy
RUN mkdir /home/wazuh-indexer && \
    apt-get update -y && \
    apt-get install curl gnupg2 -y && \
    curl -o- https://www.aptly.info/pubkey.txt | apt-key add - && \
    echo "deb http://repo.aptly.info/ squeeze main" | tee -a /etc/apt/sources.list.d/aptly.list && \
    apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y aptly build-essential cpio debhelper-compat debmake freeglut3 libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-dev libcairo2 libcairo2-dev libcups2 libdrm2 libgbm-dev libgconf-2-4 libnspr4 libnspr4-dev libnss3 libpangocairo-1.0-0 libxcomposite-dev libxdamage1 libxfixes-dev libxfixes3 libxi6 libxkbcommon-x11-0 libxrandr2 libxrender1 libxtst6 rpm rpm2cpio && \
    apt-get clean -y && \
    dpkg -r lintian && \
    addgroup --gid 1000 wazuh-indexer && \
    adduser --uid 1000 --ingroup wazuh-indexer --disabled-password --home /home/wazuh-indexer wazuh-indexer && \
    chmod 0775 /home/wazuh-indexer && \
    chown -R 1000:1000 /home/wazuh-indexer
USER wazuh-indexer
WORKDIR /home/wazuh-indexer
