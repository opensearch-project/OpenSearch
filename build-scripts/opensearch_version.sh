#!/bin/bash

set -e

grep <buildSrc/version.properties opensearch | grep -Eo '[0-9]{1,}.[0-9]{1,}.[0-9]{1,}'
