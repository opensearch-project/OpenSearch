#!/usr/bin/env bash
cd /usr/share/opensearch/bin/
./opensearch-users useradd rest_user -p test-password -r superuser || true
echo "testnode" > /tmp/password
/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/opensearch/logs/console.log
