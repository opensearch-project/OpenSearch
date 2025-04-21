#!/bin/bash
set -euo pipefail

# ====
# Print usage instructions
# ====
function usage() {
    echo "Usage: $0 <version> <stage>"
    echo "  version:  The new version to set in VERSION.json"
    echo "  stage:    The new stage to set in VERSION.json (alpha, beta, rc, stable)"
    exit 1
}

# ====
# Initialize logging
# Globals:
#   LOG_FILE
# ====
function init_logging() {
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local timestamp
    timestamp=$(date +"%Y-%m-%d_%H-%M-%S-%3N")
    LOG_FILE="$script_dir/repository_bumper_${timestamp}.log"
    exec > >(tee -a "$LOG_FILE") 2>&1
    log "Logging initialized. Log file: $LOG_FILE"
}

# ====
# Log messages with timestamp
# Arguments:
#   $1 - Message to log
# ====
function log() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1"
}

# ====
# Navigate to the root of the repository
# Searches for a folder named `.github` as a marker
# Exits if root is not found
# ====
function navigate_to_project_root() {
    local repo_root_marker=".github"
    local script_path
    script_path=$(dirname "$(realpath "$0")")

    while [[ "$script_path" != "/" ]] && [[ ! -d "$script_path/$repo_root_marker" ]]; do
        script_path=$(dirname "$script_path")
    done

    if [[ "$script_path" == "/" ]]; then
        log "Error: Unable to find the repository root."
        exit 1
    fi

    cd "$script_path"
    log "Moved to repository root: $script_path"
}

# ====
# Validate input parameters
# Arguments:
#   $1 - version
#   $2 - stage
# ====
function validate_inputs() {
    local version="$1"
    local stage="$2"

    if ! [[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        log "Error: Invalid version format '$version'."
        exit 1
    fi

    local normalized_stage
    normalized_stage=$(echo "$stage" | tr '[:upper:]' '[:lower:]')
    if ! [[ $normalized_stage =~ ^(alpha[0-9]*|beta[0-9]*|rc[0-9]*|stable)$ ]]; then
        log "Error: Invalid stage format '$stage'."
        exit 1
    fi
}

# ====
# Check if jq is installed
# ====
function check_jq_installed() {
    if ! command -v jq &>/dev/null; then
        log "Error: 'jq' is not installed. Please install it to use this script."
        exit 1
    fi
}

# ====
# Update the VERSION.json file with the new version and stage
# Arguments:
#   $1 - version
#   $2 - stage
# ====
function update_version_file() {
    local version="$1"
    local stage="$2"
    local file="VERSION.json"

    if [[ ! -f "$file" ]]; then
        log "Error: $file not found in the current directory: $(pwd)"
        exit 1
    fi

    jq --arg v "$version" --arg s "$stage" \
        '.version = $v | .stage = $s' "$file" > "${file}.tmp" && mv "${file}.tmp" "$file"

    log "Updated $file with version=$version and stage=$stage"
}

# ====
# Main logic
# ====
function main() {
    if [ "$#" -ne 2 ]; then
        usage
    fi

    local version="$1"
    local stage="$2"

    init_logging
    log "Starting update for VERSION.json with version=$version, stage=$stage"

    navigate_to_project_root
    check_jq_installed
    validate_inputs "$version" "$stage"
    update_version_file "$version" "$stage"

    log "Update complete."
}

main "$@"
