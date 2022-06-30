#!/usr/bin/env bash
set -e -o pipefail

cd /usr/share/opensearch/bin/

/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/opensearch/logs/console.log
