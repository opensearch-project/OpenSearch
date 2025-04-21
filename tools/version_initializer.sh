#!/bin/bash
set -euo pipefail

# ====
# Usage info
# ====
function usage() {
    echo "Usage: $0 <version> <previous_version>"
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
# Updates RPM spec file changelog
# Globals:
#   VERSION
#   DISTRIBUTION_FILE
# ====
function update_rpm_spec() {
    local current_date
    current_date=$(date +"%a %b %d %Y")
    local entry_line1="* $current_date support <info@wazuh.com> - $VERSION"
    local entry_line2="- More info: https://documentation.wazuh.com/current/release-notes/release-$VERSION.html"

    awk -v l1="$entry_line1" -v l2="$entry_line2" '
        BEGIN { inserted=0 }
        {
            print
            if (!inserted && /^%changelog/) {
                print l1
                print l2
                inserted=1
            }
        }
    ' "$DISTRIBUTION_FILE" > "${DISTRIBUTION_FILE}.tmp"

    mv "${DISTRIBUTION_FILE}.tmp" "$DISTRIBUTION_FILE"
    log "Updated RPM spec file: $DISTRIBUTION_FILE"
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
    local today
    today=$(date +%Y-%m-%d)
    local content
    content=$(echo "$RELEASE_NOTES_TEMPLATE_CONTENT" | sed "s|<VERSION>|$VERSION|g" | sed "s|<DATE>|$today|g")

    echo "$content" > "$RELEASE_NOTES_FILE"
    log "Created release notes: $RELEASE_NOTES_FILE"
}

# ====
# Main function
# ====
function main() {
    if [ "$#" -ne 2 ]; then
        usage
    fi

    VERSION="$1"
    PREVIOUS_VERSION="$2"

    # Paths and files
    DISTRIBUTION_FILE="distribution/packages/src/rpm/wazuh-indexer.rpm.spec"
    CHANGELOG_FILE="CHANGELOG.md"
    RELEASE_NOTES_FILE="release-notes/wazuh.release-notes-${VERSION}.md"
    FILES_TO_UPDATE=("$DISTRIBUTION_FILE" "$CHANGELOG_FILE" "$RELEASE_NOTES_FILE")

    # Log file
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local timestamp
    timestamp=$(date +"%Y-%m-%d_%H-%M-%S-%3N")
    LOG_FILE="$script_dir/version_initializer_${timestamp}.log"
    exec > >(tee -a "$LOG_FILE") 2>&1

    log "Starting version setup for $VERSION (previous: $PREVIOUS_VERSION)"
    log "Logging to $LOG_FILE"

    navigate_to_project_root
    validate_inputs
    update_rpm_spec
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
<DATE> Version <VERSION> Release Notes

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
