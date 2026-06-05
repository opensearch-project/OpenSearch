#!/bin/bash
# -----------------------------------------------------------------------------
#
# Description:
#   Downloads CTI (Cyber Threat Intelligence) snapshot .zip files from the
#   Wazuh CTI API. Consumers are discovered dynamically from the public
#   catalog plan via the /plans endpoint.
#
#   A manifest.json file is generated alongside the snapshots with metadata
#   about each consumer (name, context, type, resource URL, offset).
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
#     vulnerabilities.zip, iocs.zip, ruleset.zip
#   Plus a manifest.json with consumer metadata.
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
CTI_BASE_URL=""
OUTPUT_DIR="./snapshots"
WAZUH_TAG=""

# Consumer types we are interested in (hardcoded per requirement).
CONSUMER_TYPES=(
    "cti:catalog:consumer:vulnerabilities"
    "cti:catalog:consumer:iocs"
    "cti:catalog:consumer:ruleset"
)

# =============================================================================
# Usage
# =============================================================================
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Downloads CTI snapshot .zip files for all public consumers and generates
a manifest.json with consumer metadata.

Options:
  --env <base-url>        CTI API base URL (e.g. https://<your-environment>/api/v1).
  --output-dir <path>     Output directory (default: ./snapshots).
  --version <tag>         Product version tag (e.g. 5.0.0-rc1). Auto-detected from VERSION.json if omitted.
  --help                  Show this help message and exit.

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
        --version)
            WAZUH_TAG="${2:?ERROR: --version requires a version tag}"
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

# Strip trailing slash
CTI_BASE_URL="${CTI_BASE_URL%/}"

# Auto-detect version from VERSION.json if not provided
if [[ -z "${WAZUH_TAG}" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    VERSION_FILE="${SCRIPT_DIR}/../VERSION.json"
    if [[ -f "${VERSION_FILE}" ]]; then
        _ver=$(jq -r '.version // empty' "${VERSION_FILE}")
        _stage=$(jq -r '.stage // empty' "${VERSION_FILE}")
        if [[ -n "${_ver}" ]]; then
            WAZUH_TAG="${_ver}"
            if [[ -n "${_stage}" ]]; then
                WAZUH_TAG="${_ver}-${_stage}"
            fi
        fi
    fi
    if [[ -z "${WAZUH_TAG}" ]]; then
        echo "ERROR: Could not determine product version. Provide --version or ensure VERSION.json exists."
        exit 1
    fi
fi

echo "Product version tag: ${WAZUH_TAG}"

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
# Helper: api_get <url>
#   Makes a GET request with retries. Returns the response body.
# =============================================================================
api_get() {
    local url="$1"
    curl -fsSL --retry 3 --retry-delay 5 --connect-timeout 10 --max-time 30 \
        -H "wazuh-tag: v${WAZUH_TAG}" "${url}"
    }

# =============================================================================
# Helper: extract_context <resource_url>
#   Extracts the context from a resource URL of the form:
#   .../contexts/<context>/consumers/<name>
# =============================================================================
extract_context() {
    local url="$1"
    # Match /contexts/<value>/
    if [[ "${url}" =~ /contexts/([^/]+)/ ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo "ERROR: Cannot extract context from resource URL: ${url}" >&2
        return 1
    fi
}

# =============================================================================
# Helper: type_to_filename <type>
#   Derives the .zip filename from the consumer type.
#   e.g. cti:catalog:consumer:ruleset -> ruleset.zip
# =============================================================================
type_to_filename() {
    local type="$1"
    echo "${type##*:}.zip"
}

# =============================================================================
# Helper: fetch_public_plan
#   Fetches the public plan from the /plans endpoint.
# =============================================================================
fetch_public_plan() {
    local plans_url="${CTI_BASE_URL}/catalog/plans"
    echo "Fetching catalog plans from ${plans_url}" >&2

    local response
    response=$(api_get "${plans_url}") || {
        echo "ERROR: Failed to fetch plans from ${plans_url}"
        exit 1
    }

    # Find public plans
    local public_plans
    public_plans=$(echo "${response}" | jq '[.plans[] | select(.is_public == true)]')

    local count
    count=$(echo "${public_plans}" | jq 'length')

    if [[ "${count}" -ne 1 ]]; then
        echo "ERROR: Expected exactly 1 public plan, found ${count}"
        exit 1
    fi

    echo "${public_plans}" | jq '.[0]'
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo "Wazuh CTI Snapshot Downloader"
    echo "CTI API URL: ${CTI_BASE_URL}"
    echo ""

    mkdir -p "${OUTPUT_DIR}"
    rm -f "${OUTPUT_DIR}"/*.zip "${OUTPUT_DIR}"/manifest.json

    local plan
    plan=$(fetch_public_plan)

    # Start building manifest as a JSON object
    local manifest="{}"

    for consumer_type in "${CONSUMER_TYPES[@]}"; do
        local filename
        filename=$(type_to_filename "${consumer_type}")

        echo ""
        echo "--- Processing ${consumer_type} ---"

        # Find the feature matching this type
        local feature
        feature=$(echo "${plan}" | jq -r --arg t "${consumer_type}" '.features[] | select(.type == $t)')

        if [[ -z "${feature}" || "${feature}" == "null" ]]; then
            echo "ERROR: No feature found for type ${consumer_type} in public plan"
            exit 1
        fi

        local name resource_url
        name=$(echo "${feature}" | jq -r '.name')
        resource_url=$(echo "${feature}" | jq -r '.resource')

        echo "  Consumer: ${name}"
        echo "  Resource: ${resource_url}"

        # Fetch consumer details
        local consumer_data
        consumer_data=$(api_get "${resource_url}") || {
            echo "ERROR: Failed to fetch consumer details from ${resource_url}"
            exit 1
        }

        local snapshot_link snapshot_offset changes_url is_public
        snapshot_link=$(echo "${consumer_data}" | jq -r '.data.last_snapshot_link // empty')
        snapshot_offset=$(echo "${consumer_data}" | jq -r '.data.last_snapshot_offset // empty')
        is_public=$(echo "${consumer_data}" | jq -r '.data.is_public // empty')

        if [[ -z "${snapshot_link}" ]]; then
            echo "ERROR: No snapshot link found for ${name}"
            exit 1
        fi

        local context
        context=$(extract_context "${resource_url}")

        echo "  Context: ${context}"
        echo "  Snapshot offset: ${snapshot_offset}"

        # Download snapshot
        local zip_file="${OUTPUT_DIR}/${filename}"
        echo "  Downloading snapshot from ${snapshot_link}"
        curl -fSL --retry 3 --retry-delay 5 --connect-timeout 15 --max-time 300 \
            -H "wazuh-tag: v${WAZUH_TAG}" \
            -o "${zip_file}" "${snapshot_link}" || {
            echo "ERROR: Failed to download snapshot for ${name}"
            exit 1
        }
        echo "  Saved to ${zip_file}"

        # Add to manifest
        manifest=$(echo "${manifest}" | jq \
            --arg file "${filename}" \
            --arg name "${name}" \
            --arg ctx "${context}" \
            --arg type "${consumer_type}" \
            --arg res "${resource_url}" \
            --argjson pub "${is_public}" \
            --argjson off "${snapshot_offset}" \
            '. + {($file): {name: $name, context: $ctx, type: $type, resource: $res, is_public: $pub, remote_offset: $off}}')
    done

    # Write manifest
    echo "${manifest}" | jq '.' > "${OUTPUT_DIR}/manifest.json"
    echo ""
    echo "Manifest written to ${OUTPUT_DIR}/manifest.json"

    # Summary
    echo ""
    echo "===== Summary ====="
    echo "Output directory: ${OUTPUT_DIR}"
    echo ""
    for f in "${OUTPUT_DIR}"/*.zip; do
        [[ -f "${f}" ]] || continue
        local size
        size=$(du -h "${f}" | cut -f1)
        printf "  %-30s %s\n" "$(basename "${f}")" "${size}"
    done
}

main
