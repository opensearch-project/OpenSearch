#!/usr/bin/env bash
cd /usr/share/opensearch/bin/

/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/opensearch/logs/console.log
