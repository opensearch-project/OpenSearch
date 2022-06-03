#!/usr/bin/env bash
set -e -o pipefail
<% commands.each {command -> %><%= command %><% } %>
