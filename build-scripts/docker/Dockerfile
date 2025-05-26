# Wazuh Docker Copyright (C) 2017, Wazuh Inc. (License GPLv2)
FROM amazonlinux:2023.6.20241212.0 AS builder

ARG VERSION
ARG INDEXER_TAR_NAME

RUN yum install openssl tar findutils shadow-utils jq -y

COPY ${INDEXER_TAR_NAME} /

COPY config/opensearch.yml /

COPY config/config.yml /

COPY config/config.sh /

RUN bash config.sh

################################################################################
# Build stage 1 (the actual Wazuh indexer image):
#
# Copy wazuh-indexer from stage 0
# Add entrypoint

################################################################################
FROM amazonlinux:2023.6.20241212.0

ENV USER="wazuh-indexer" \
    GROUP="wazuh-indexer" \
    NAME="wazuh-indexer" \
    INSTALL_DIR="/usr/share/wazuh-indexer"

RUN yum install curl-minimal shadow-utils findutils hostname jq -y

RUN getent group $GROUP || groupadd -r -g 1000 $GROUP

RUN useradd --system \
    --uid 1000 \
    --no-create-home \
    --home-dir $INSTALL_DIR \
    --gid $GROUP \
    --shell /sbin/nologin \
    --comment "$USER user" \
    $USER

WORKDIR $INSTALL_DIR

COPY entrypoint.sh /

COPY config/securityadmin.sh /

RUN chmod 700 /entrypoint.sh && chmod 700 /securityadmin.sh

RUN chown 1000:1000 /*.sh

COPY --from=builder --chown=1000:1000 /debian/wazuh-indexer/usr/share/wazuh-indexer /usr/share/wazuh-indexer
COPY --from=builder --chown=0:0 /debian/wazuh-indexer/usr/lib/systemd /usr/lib/systemd
COPY --from=builder --chown=0:0 /debian/wazuh-indexer/usr/lib/sysctl.d /usr/lib/sysctl.d
COPY --from=builder --chown=0:0 /debian/wazuh-indexer/usr/lib/tmpfiles.d /usr/lib/tmpfiles.d

RUN chown -R 1000:1000 /usr/share/wazuh-indexer

RUN mkdir -p /var/lib/wazuh-indexer && chown 1000:1000 /var/lib/wazuh-indexer && \
    mkdir -p /usr/share/wazuh-indexer/logs && chown 1000:1000 /usr/share/wazuh-indexer/logs && \
    mkdir -p /run/wazuh-indexer && chown 1000:1000 /run/wazuh-indexer && \
    mkdir -p /var/log/wazuh-indexer && chown 1000:1000 /var/log/wazuh-indexer && \
    chmod 700 /usr/share/wazuh-indexer && \
    chmod 600 /usr/share/wazuh-indexer/config/jvm.options && \
    chmod 600 /usr/share/wazuh-indexer/config/opensearch.yml

USER wazuh-indexer

# Services ports
EXPOSE 9200

ENTRYPOINT ["/entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint
CMD ["opensearchwrapper"]
