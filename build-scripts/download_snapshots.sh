#!/bin/bash
# -----------------------------------------------------------------------------
#
# Description:
#   Downloads CTI (Cyber Threat Intelligence) snapshot .zip files from the
#   Wazuh CTI API for all three consumers (ruleset, IoC, CVE) and saves them
#   to the output directory without extracting.
#
#   The downloaded snapshots can later be loaded into the Content Manager
#   plugin for indexing.
#
# Usage:
#   ./download_snapshots.sh --env <base-url> [--output-dir <path>] [--help]
#
# Arguments:
#   --env <base-url>         CTI API base URL (e.g. https://<your-environment>/api/v1).
#   --output-dir <path>      Directory for the output .zip files (default: ./snapshots).
#   --help                   Show this help message and exit.
#
# Requirements:
#   - curl
#   - jq
#
# Output:
#   One .zip file per consumer under the output directory:
#     content.zip, ioc.zip, cve.zip
#
# Exit Codes:
#   0   Success
#   1   Missing dependency, bad arguments, or download failure
#
# -----------------------------------------------------------------------------

set -euo pipefail

# =============================================================================
# Defaults
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTI_BASE_URL=""
CONTENT_SOURCES="${SCRIPT_DIR}/feeds.json"
OUTPUT_DIR="./snapshots"

# =============================================================================
# Usage
# =============================================================================
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Downloads CTI snapshot .zip files for all consumers (content, IoC, CVE).

Options:
  --env <base-url>        CTI API base URL (e.g. https://<your-environment>/api/v1).
  --output-dir <path>     Output directory (default: ./snapshots).
  --help                  Show this help message and exit.

The script reads consumer definitions from feeds.json located
in the same directory as this script. Each key becomes the output filename (<key>.zip).

Example:
  $(basename "$0") --env https://<your-environment>/api/v1
EOF
    exit 0
}

# =============================================================================
# Argument parsing
# =============================================================================
while [[ $# -gt 0 ]]; do
    case "$1" in
        --env)
            CTI_BASE_URL="${2:?ERROR: --env requires a CTI API base URL}"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="${2:?ERROR: --output-dir requires a path}"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            echo "ERROR: Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required --env parameter
if [[ -z "${CTI_BASE_URL}" ]]; then
    echo "ERROR: --env is required. Provide the CTI API base URL."
    echo ""
    usage
fi

# Validate feeds file exists
if [[ ! -f "${CONTENT_SOURCES}" ]]; then
    echo "ERROR: Content sources file not found: ${CONTENT_SOURCES}"
    exit 1
fi

# =============================================================================
# Dependency checks
# =============================================================================
for cmd in curl jq; do
    if ! command -v "${cmd}" &>/dev/null; then
        echo "ERROR: Required command '${cmd}' not found. Please install it."
        exit 1
    fi
done

# =============================================================================
# Helper: fetch_snapshot_url <context> <consumer>
#   Queries the CTI API and returns the snapshot download URL.
# =============================================================================
fetch_snapshot_url() {
    local base_url="$1"
    local context="$2"
    local consumer="$3"

    # Strip trailing slash to avoid double slashes in the URL
    base_url="${base_url%/}"
    local url="${base_url}/catalog/contexts/${context}/consumers/${consumer}"

    echo "  Fetching consumer metadata from ${url}" >&2
    local response
    response=$(curl -fsSL --retry 3 --retry-delay 5 --connect-timeout 10 --max-time 30 "${url}") || {
        echo "ERROR: Failed to fetch consumer ${context}/${consumer}" >&2
        return 1
    }

    local snapshot_url
    snapshot_url=$(echo "${response}" | jq -r '.data.last_snapshot_link // empty')

    if [[ -z "${snapshot_url}" ]]; then
        echo "ERROR: No snapshot link found for ${context}/${consumer}" >&2
        return 1
    fi

    # The API may return snapshot URLs with a different host than the one
    # we queried (e.g. cti-pre.wazuh.com instead of cti.pre.cloud.wazuh.com).
    # Rewrite the host to match the base URL we were given.
    local base_host="${base_url#https://}"
    base_host="${base_host%%/*}"
    local snap_host="${snapshot_url#https://}"
    snap_host="${snap_host%%/*}"
    if [[ "${snap_host}" != "${base_host}" ]]; then
        echo "  Rewriting snapshot host: ${snap_host} -> ${base_host}" >&2
        snapshot_url="${snapshot_url/${snap_host}/${base_host}}"
    fi

    echo "${snapshot_url}"
}

# =============================================================================
# Helper: download_snapshot <label> <snapshot_url>
#   Downloads a snapshot .zip file to the output directory.
# =============================================================================
download_snapshot() {
    local label="$1"
    local snapshot_url="$2"
    local zip_file="${OUTPUT_DIR}/${label}.zip"

    echo "  Downloading ${label} snapshot from ${snapshot_url}" >&2
    curl -fSL --retry 3 --retry-delay 5 --connect-timeout 15 --max-time 300 -o "${zip_file}" "${snapshot_url}" || {
        echo "ERROR: Failed to download snapshot for ${label}" >&2
        return 1
    }

    echo "  Saved to ${zip_file}" >&2
}

# =============================================================================
# Helper: print_summary <output_dir>
#   Prints a file-size summary for every downloaded .zip file.
# =============================================================================
print_summary() {
    local output_dir="$1"

    echo ""
    echo "===== Summary ====="
    echo "Output directory: ${output_dir}"
    echo ""

    for f in "${output_dir}"/*.zip; do
        [[ -f "${f}" ]] || continue
        local size
        size=$(du -h "${f}" | cut -f1)
        printf "  %-25s %s\n" "$(basename "${f}")" "${size}"
    done
}

# =============================================================================
# Helper: process_consumer <label> <context> <consumer>
#   Fetches the snapshot URL and downloads the .zip file.
# =============================================================================
process_consumer() {
    local label="$1"
    local base_url="$2"
    local context="$3"
    local consumer="$4"

    echo ""
    echo "--- Processing consumer: ${label} (${context}/${consumer}) ---"

    local snapshot_url
    snapshot_url=$(fetch_snapshot_url "${base_url}" "${context}" "${consumer}") || return 1

    download_snapshot "${label}" "${snapshot_url}"

    echo "  Done with ${label}."
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo "Wazuh CTI Snapshot Downloader"
    echo "CTI API URL:       ${CTI_BASE_URL}"
    echo "Content sources:   ${CONTENT_SOURCES}"

    mkdir -p "${OUTPUT_DIR}"

    # Remove any previous snapshot .zip files to avoid stale data
    rm -f "${OUTPUT_DIR}"/*.zip

    # Iterate over every entry in the feeds JSON file.
    # Each key is used as the snapshot label and output filename.
    local keys
    keys=$(jq -r 'keys[]' "${CONTENT_SOURCES}")
    for key in ${keys}; do
        local context consumer
        context=$(jq -r --arg k "${key}" '.[$k].context' "${CONTENT_SOURCES}")
        consumer=$(jq -r --arg k "${key}" '.[$k].consumer' "${CONTENT_SOURCES}")
        process_consumer "${key}" "${CTI_BASE_URL}" "${context}" "${consumer}"
    done

    # Print summary
    print_summary "${OUTPUT_DIR}"
}

main
