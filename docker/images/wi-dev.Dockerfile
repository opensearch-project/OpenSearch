FROM gradle:jdk17-alpine AS builder
USER gradle
WORKDIR /home/wazuh-indexer/app
COPY --chown=gradle:gradle . /home/wazuh-indexer/app
RUN gradle clean


FROM eclipse-temurin:17-jdk-alpine
RUN apk add git && \
    addgroup -g 1000 wazuh-indexer && \
    adduser -u 1000 -G wazuh-indexer -D -h /home/wazuh-indexer wazuh-indexer && \
    chmod 0775 /home/wazuh-indexer && \
    chown -R 1000:0 /home/wazuh-indexer
USER wazuh-indexer
COPY --from=builder --chown=1000:0 /home/wazuh-indexer/app /home/wazuh-indexer/app
WORKDIR /home/wazuh-indexer/app
RUN git config --global --add safe.directory /home/wazuh-indexer/app
EXPOSE 9200 9300
