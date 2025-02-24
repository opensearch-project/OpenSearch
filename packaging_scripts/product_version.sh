#!/bin/bash

## Check that jq is installed
if [ ! -e '/usr/bin/jq' ]
then
  echo "ERROR: jq command could not be found under /usr/bin"
  exit 1
fi

cd "$(dirname "${BASH_SOURCE[0]}")"

jq -r .version ../VERSION.json

exit 0
