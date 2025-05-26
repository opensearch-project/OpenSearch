#!/bin/bash

# Check if VERSION file has exactly one line and that line is non-empty
if [ "$(jq -e .version VERSION.json)" -ne 0 ]; then
    echo "Error: VERSION file must contain a 'version' field."
    exit 1
fi
if [ "$(jq -e .stage VERSION.json)" -ne 0 ]; then
    echo "Error: VERSION file must contain a 'stage' field."
    exit 1
fi
