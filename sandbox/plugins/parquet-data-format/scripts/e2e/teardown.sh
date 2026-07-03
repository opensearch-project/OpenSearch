#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../.." && pwd)"
CLUSTER_PID_FILE="${SCRIPT_DIR}/.cluster.pid"
CLUSTER_LOG_FILE="${SCRIPT_DIR}/.cluster.log"
TESTCLUSTER_PATH="${REPO_ROOT}/build/testclusters/runTask-0"

info()  { echo "[teardown] $*"; }
error() { echo "[teardown] ERROR: $*" >&2; }

stopped_any=false

wait_for_exit() {
    local pid="$1"
    local timeout=15
    local elapsed=0
    while kill -0 "${pid}" 2>/dev/null; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [[ ${elapsed} -ge ${timeout} ]]; then
            info "Process ${pid} did not stop cleanly — sending SIGKILL."
            kill -9 "${pid}" 2>/dev/null || true
            break
        fi
    done
}

stop_recorded_gradle_pid() {
    if [[ ! -f "${CLUSTER_PID_FILE}" ]]; then
        info "No .cluster.pid file found."
        return
    fi

    local pid
    pid=$(cat "${CLUSTER_PID_FILE}")
    if ! kill -0 "${pid}" 2>/dev/null; then
        info "Process ${pid} is already gone."
        rm -f "${CLUSTER_PID_FILE}"
        return
    fi

    info "Stopping Gradle run process (PID ${pid}) ..."

    # In non-interactive scripts the Gradle child can share our process group.
    # Killing that group terminates run-all.sh before it can clean up orphaned
    # OpenSearch JVMs, so only use group termination when the group is distinct.
    local pgid current_pgid
    pgid=$(ps -o pgid= -p "${pid}" 2>/dev/null | tr -d ' ')
    current_pgid=$(ps -o pgid= -p "$$" 2>/dev/null | tr -d ' ')
    if [[ -n "${pgid}" && "${pgid}" != "0" && "${pgid}" != "${current_pgid}" ]]; then
        kill -- "-${pgid}" 2>/dev/null || kill "${pid}" 2>/dev/null || true
    else
        kill "${pid}" 2>/dev/null || true
    fi

    wait_for_exit "${pid}"
    rm -f "${CLUSTER_PID_FILE}"
    stopped_any=true
}

stop_orphaned_opensearch_pids() {
    local pid
    local found=false
    local pattern="opensearch.path.home=${TESTCLUSTER_PATH}/distro"
    local pids
    pids=$(pgrep -f "${pattern}" 2>/dev/null || true)

    for pid in ${pids}; do
        if [[ "${pid}" == "$$" ]]; then
            continue
        fi
        if kill -0 "${pid}" 2>/dev/null; then
            found=true
            info "Stopping orphaned OpenSearch testcluster process (PID ${pid}) ..."
            kill "${pid}" 2>/dev/null || true
            wait_for_exit "${pid}"
            stopped_any=true
        fi
    done

    if [[ "${found}" == false ]]; then
        info "No orphaned OpenSearch testcluster processes found."
    fi
}

stop_recorded_gradle_pid
stop_orphaned_opensearch_pids

if [[ "${stopped_any}" == true ]]; then
    info "Cluster stopped."
else
    info "Nothing to stop."
fi

if [[ -f "${CLUSTER_LOG_FILE}" ]]; then
    info "Log file retained at: ${CLUSTER_LOG_FILE}"
fi
