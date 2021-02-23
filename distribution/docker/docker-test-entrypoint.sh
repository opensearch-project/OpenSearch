#!/bin/bash
cd /usr/share/elasticsearch/bin/
./elasticsearch-users useradd rest_user -p test-password -r superuser || true
echo "testnode" > /tmp/password
/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/elasticsearch/logs/console.log
