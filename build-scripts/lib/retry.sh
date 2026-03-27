#!/bin/bash

# Shared retry utility with exponential backoff.
#
# Usage:
#   source "$(dirname "${BASH_SOURCE[0]}")/lib/retry.sh"
#   retry 3 5 curl -fsSL https://example.com/file -o /tmp/file
#
# Arguments:
#   $1  max_attempts    Maximum number of attempts (e.g. 3)
#   $2  initial_delay   Initial delay in seconds between retries (doubles each attempt)
#   $@  command          The command to execute

retry() {
    local max_attempts="$1"
    local delay="$2"
    shift 2

    local cmd_label="${1##*/}"
    local attempt=1
    while true; do
        if "$@"; then
            return 0
        fi
        if (( attempt >= max_attempts )); then
            echo "ERROR: Command '${cmd_label}' failed after ${max_attempts} attempts." >&2
            return 1
        fi
        echo "WARNING: Command '${cmd_label}' attempt ${attempt}/${max_attempts} failed. Retrying in ${delay}s..." >&2
        sleep "$delay"
        delay=$(( delay * 2 ))
        attempt=$(( attempt + 1 ))
    done
}
