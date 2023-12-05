#!/usr/bin/env bash
set -e -o pipefail

cd /usr/share/wazuh-indexer/bin/

/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/wazuh-indexer/logs/console.log
