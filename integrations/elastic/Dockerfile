FROM opensearchproject/logstash-oss-with-opensearch-output-plugin:latest

ENV LOGSTASH_KEYSTORE_PASS "SecretPassword"
ENV LS_PATH "/usr/share/logstash"
USER logstash

# https://github.com/elastic/logstash/issues/6600
# Install plugin
RUN LS_JAVA_OPTS="-Xms1024m -Xmx1024m" logstash-plugin install logstash-input-opensearch

COPY --chown=logstash:logstash logstash/pipeline /usr/share/logstash/pipeline
# Copy and run the setup.sh script to create and configure a keystore for Logstash.
COPY --chown=logstash:logstash logstash/setup.sh /usr/share/logstash/bin/setup.sh
RUN bash /usr/share/logstash/bin/setup.sh

# Disable ECS compatibility
RUN `echo "pipeline.ecs_compatibility: disabled" >> /usr/share/logstash/config/logstash.yml`

WORKDIR /usr/share/logstash