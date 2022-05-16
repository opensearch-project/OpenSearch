#!/usr/bin/env bash

set -e -o pipefail

addprinc.sh opensearch
addprinc.sh HTTP/localhost
addprinc.sh peppa
addprinc.sh george          dino

# Use this as a signal that setup is complete
python3 -m http.server 4444 &

sleep infinity
