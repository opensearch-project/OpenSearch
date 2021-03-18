#!/bin/bash

set -e

addprinc.sh "opensearch"
#TODO(OpenSearch): fix username
addprinc.sh "hdfs/hdfs.build.opensearch.co"

# Use this as a signal that setup is complete
python3 -m http.server 4444 &

sleep infinity
