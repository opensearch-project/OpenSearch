#!/bin/bash
cd /usr/share/opensearch/bin/
echo "testnode" > /tmp/password
/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/opensearch/logs/console.log
