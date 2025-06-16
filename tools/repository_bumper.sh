#!/bin/bash

# =========================
# Repository Bumper Script
# =========================
# This script updates the VERSION.json file and the changelog section of the
# RPM spec file for a new version release.
#
# It takes three arguments:
# 1. The new version to set (e.g., 4.5.0)
# 2. The new stage to set (alpha, beta, rc, stable)
# 3. The date to set in the changelog (e.g., 'Mon Jan 02 2025')
#
# The changelog entry will be added to the %changelog section of the RPM spec file,
# and will be formatted as follows:
#   * [DATE] support <info@wazuh.com> - [VERSION]
#   - More info: https://documentation.wazuh.com/current/release-notes/release-[VERSION].html

set -euo pipefail

# ====
# Print usage instructions
# ====
function usage() {
    echo "Usage: $0 <version> <stage> <date>"
    echo "  version:  The new version to set in VERSION.json (e.g., 4.5.0)"
    echo "  stage:    The new stage to set in VERSION.json (alpha, beta, rc, stable)"
    echo "  date:     The date to set in the changelog (e.g., '2025-04-13')"
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
#   $3 - date
# ====
function validate_inputs() {
    local version="$1"
    local stage="$2"
    local date="$3"

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

    if ! [[ $date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        log "Error: Invalid date format $date."
        exit 1
    fi
}

# ====
# Changes the date format to the format used in the changelog files
# ====
function normalize_date() {
    local input_date="$1"
    local normalized=""

    if date --version >/dev/null 2>&1; then
        # GNU date (Linux)
        normalized=$(LC_TIME=en_US.UTF-8 date -d "$input_date" +"%a %b %d %Y")
    else
        # BSD date (macOS)
        normalized=$(LC_TIME=en_US.UTF-8 date -jf "%Y-%m-%d" "$input_date" +"%a %b %d %Y")
    fi

    echo "$normalized"
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
# Update the changelog section of the RPM spec file, if needed
# Arguments:
#   $1 - version
#   $2 - date
# ====
function update_rpm_changelog() {
    local version="$1"
    local date="$2"
    local spec_file="distribution/packages/src/rpm/wazuh-indexer.rpm.spec"
    local changelog_entry="* $date support <info@wazuh.com> - $version"

    if grep -q "^- More info: .*release-$version\.html" "$spec_file"; then
        # Update existing changelog date
        awk -v version="$version" -v new_date="$date" '
            BEGIN { updated = 0 }
            {
                if ($0 ~ "^- More info: .*release-"version"\\.html") {
                    prev = NR - 1
                    lines[prev] = "* " new_date " support <info@wazuh.com> - " version
                    lines[NR] = $0
                    updated = 1
                } else {
                    lines[NR] = $0
                }
            }
            END {
                for (i = 1; i <= NR; i++) print lines[i]
            }
        ' "$spec_file" >"${spec_file}.tmp" && mv "${spec_file}.tmp" "$spec_file"

        log "Updated existing changelog entry for version=$version with date=$date"
    else
        log "Inserting changelog entry for version=$version"
        awk -v line1="$changelog_entry" -v line2="- More info: https://documentation.wazuh.com/current/release-notes/release-$version.html" '
        BEGIN { inserted=0 }
        {
            print
            if (!inserted && /^%changelog/) {
                print line1
                print line2
                inserted=1
            }
        }
        ' "$spec_file" >"${spec_file}.tmp" && mv "${spec_file}.tmp" "$spec_file"

        log "Inserted new changelog entry for version=$version with date=$date"
    fi
}

# ====
# Main logic
# ====
function main() {
    if [ "$#" -ne 3 ]; then
        log "Error: Invalid number of arguments. Expected 3, got $#."
        usage
    fi

    local version="$1"
    local stage="$2"
    local date="$3"

    init_logging
    log "Starting update for VERSION.json with version=$version, stage=$stage"

    navigate_to_project_root
    check_jq_installed
    validate_inputs "$version" "$stage" "$date"
    date=$(normalize_date "$date")
    update_version_file "$version" "$stage"
    update_rpm_changelog "$version" "$date"

    log "Update complete."
}

main "$@"
