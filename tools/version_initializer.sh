#!/bin/bash

# =========================
# Version Initializer Script
# =========================
# This script initializes the changelog and release notes for a new version.
# It should be used alongside the repository_bumper.sh script in order to
# ensure that all the necessary files are updated correctly.
#
# The script takes three arguments:
# 1. The new version to set (e.g., 4.5.0)
# 2. The previous version to compare against (e.g., 4.4.0)
# 3. The date to set in the release notes (e.g., '2025-01-01')
#
# It will create or update the CHANGELOG.md and release-notes files with the
# new version information. Both files will be formatted according to the
# [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) standard.

set -euo pipefail

# ====
# Usage info
# ====
function usage() {
    echo "Usage: $0 <version> <previous_version> <date>"
    echo "  version:            The new version to set (e.g., 4.5.0)"
    echo "  previous_version:   The previous version to compare against (e.g., 4.4.0)"
    echo "  date:               The date to set in the release notes (e.g., '2025-01-01')"
    exit 1
}

# ====
# Log a message with timestamp
# Globals:
#   LOG_FILE
# Arguments:
#   $1 - The message to log
# ====
function log() {
    local message="$1"
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $message" | tee -a "$LOG_FILE"
}

# ====
# Validates input version formats
# Globals:
#   VERSION
#   PREVIOUS_VERSION
# ====
function validate_inputs() {
    if ! [[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        log "Error: Invalid version format '$VERSION'."
        exit 1
    fi

    if ! [[ $PREVIOUS_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        log "Error: Invalid previous version format '$PREVIOUS_VERSION'."
        exit 1
    fi

    if ! [[ $DATE =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        log "Error: Invalid date format '$DATE'."
        exit 1
    fi
}

# ====
# Navigates to the project root directory
# Exits if root marker not found
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
    log "Changed working directory to repo root: $script_path"
}

# ====
# Initializes CHANGELOG.md for new version
# Globals:
#   CHANGELOG_FILE
#   VERSION
#   PREVIOUS_VERSION
# ====
function update_changelog() {
    local content
    content=$(echo "$CHANGELOG_TEMPLATE_CONTENT" | sed "s|<VERSION>|$VERSION|g" | sed "s|<PREVIOUS_VERSION>|$PREVIOUS_VERSION|g")

    echo "$content" > "$CHANGELOG_FILE"
    log "Initialized changelog: $CHANGELOG_FILE"
}

# ====
# Creates new release note file
# Globals:
#   RELEASE_NOTES_FILE
#   VERSION
# ====
function update_release_notes() {
    local content
    content=$(echo "$RELEASE_NOTES_TEMPLATE_CONTENT" | sed "s|<VERSION>|$VERSION|g" | sed "s|<DATE>|$DATE|g")

    echo "$content" > "$RELEASE_NOTES_FILE"
    log "Created release notes: $RELEASE_NOTES_FILE"
}

# ====
# Main function
# ====
function main() {
    if [ "$#" -ne 3 ]; then
        usage
    fi

    VERSION="$1"
    PREVIOUS_VERSION="$2"
    DATE="$3"

    # Paths and files
    CHANGELOG_FILE="CHANGELOG.md"
    RELEASE_NOTES_FILE="release-notes/wazuh.release-notes-${VERSION}.md"
    FILES_TO_UPDATE=("$CHANGELOG_FILE" "$RELEASE_NOTES_FILE")

    # Log file
    local script_dir
    local timestamp
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    timestamp=$(date +"%Y-%m-%d_%H-%M-%S-%3N")
    LOG_FILE="$script_dir/version_initializer_${timestamp}.log"
    exec > >(tee -a "$LOG_FILE") 2>&1

    log "Starting version setup for $VERSION (previous: $PREVIOUS_VERSION)"
    log "Logging to $LOG_FILE"

    navigate_to_project_root
    validate_inputs
    update_changelog
    update_release_notes

    log "All updates completed successfully."
    log "Updated files:"
    for file in "${FILES_TO_UPDATE[@]}"; do
        log " - $file"
    done
}

# Templates
CHANGELOG_TEMPLATE_CONTENT=$(cat <<'EOF'
# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [<VERSION>]
### Added
- 

### Dependencies
-

### Changed
- 

### Deprecated
-

### Removed
- 

### Fixed
- 

### Security
- 

[Unreleased <VERSION>]: https://github.com/wazuh/wazuh-indexer/compare/<PREVIOUS_VERSION>...<VERSION>
EOF
)

RELEASE_NOTES_TEMPLATE_CONTENT=$(cat <<'EOF'
## <DATE> Version <VERSION> Release Notes

## [<VERSION>]
### Added
-

### Dependencies
-

### Changed
-

### Deprecated
-

### Removed
-

### Fixed
-

### Security
-
EOF
)

# Init
main "$@"
