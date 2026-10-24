#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -e -o pipefail

cd /usr/share/opensearch/bin/

/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/opensearch/logs/console.log
