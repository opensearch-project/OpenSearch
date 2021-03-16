#!/bin/bash
cd /usr/share/opensearch/bin/
## TODO remove this line as it's x-pack
./elasticsearch-users useradd rest_user -p test-password -r superuser || true
echo "testnode" > /tmp/password
/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/opensearch/logs/console.log
