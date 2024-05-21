#!/bin/bash


set -e

< buildSrc/version.properties grep opensearch | grep -Eo '[0-9]{1,}.[0-9]{1,}.[0-9]{1,}'
