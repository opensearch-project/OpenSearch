FROM eclipse-temurin:23-jdk-alpine
RUN apk add git && \
  apk add curl && \
  apk add bash && \
  apk add jq && \
  addgroup -g 1000 wazuh-indexer && \
  adduser -u 1000 -G wazuh-indexer -D -h /home/wazuh-indexer wazuh-indexer && \
  chmod 0775 /home/wazuh-indexer && \
  chown -R 1000:0 /home/wazuh-indexer
USER wazuh-indexer
COPY --chown=1000:0 . /home/wazuh-indexer
WORKDIR /home/wazuh-indexer
RUN ./gradlew clean
RUN git config --global --add safe.directory /home/wazuh-indexer
EXPOSE 9200 9300
