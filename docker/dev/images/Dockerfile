FROM gradle:jdk17-alpine AS builder
USER gradle
WORKDIR /home/wazuh-indexer
COPY --chown=gradle:gradle . /home/wazuh-indexer
RUN gradle clean


FROM eclipse-temurin:17-jdk-alpine
RUN apk add git && \
    addgroup -g 1000 wazuh-indexer && \
    adduser -u 1000 -G wazuh-indexer -D -h /home/wazuh-indexer wazuh-indexer && \
    chmod 0775 /home/wazuh-indexer && \
    chown -R 1000:0 /home/wazuh-indexer
USER wazuh-indexer
COPY --from=builder --chown=1000:0 /home/wazuh-indexer /home/wazuh-indexer
WORKDIR /home/wazuh-indexer
RUN git config --global --add safe.directory /home/wazuh-indexer
EXPOSE 9200 9300
