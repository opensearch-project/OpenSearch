#!/bin/bash

# -----------------------------------------------------------------------------
# Script Name: check_compatibility_versions.sh
#
# Description:
#   Validates version compatibility between Wazuh Indexer and dependent
#   repositories. It checks both:
#     1) Product version
#     2) OpenSearch version
#
#   Repository version data is provided as line-based input in the
#   PLUGIN_VERSIONS variable using this format per line:
#     <repo-name>|<repo-version>|<repo-opensearch-version>
#
# Required environment variables:
#   INDEXER_VERSION      Wazuh Indexer version to compare against.
#   OPENSEARCH_VERSION   Wazuh Indexer OpenSearch version to compare against.
#   PLUGIN_VERSIONS      Multi-line repository data in pipe-delimited format.
#
# Usage:
#   INDEXER_VERSION="5.0.0" \
#   OPENSEARCH_VERSION="3.2.0" \
#   PLUGIN_VERSIONS=$'repo-a|5.0.0|3.2.0\nrepo-b|5.0.0|3.2.0' \
#   bash build-scripts/check_compatibility_versions.sh
#
# Exit codes:
#   0  Compatibility checks passed.
#   1  Missing required variables or compatibility mismatch detected.
# -----------------------------------------------------------------------------

set -euo pipefail

required_vars=(
  "INDEXER_VERSION"
  "OPENSEARCH_VERSION"
  "PLUGIN_VERSIONS"
)

for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "ERROR: Required variable '$var' is not set."
    exit 1
  fi
done

version_mismatches=0
opensearch_mismatches=0

while IFS='|' read -r repo repo_version repo_opensearch_version; do
  [[ -z "${repo}" ]] && continue

  if [[ "${INDEXER_VERSION}" != "${repo_version}" ]]; then
    echo "Version mismatch in ${repo}: indexer='${INDEXER_VERSION}', repo='${repo_version}'"
    version_mismatches=1
  fi

  if [[ "${OPENSEARCH_VERSION}" != "${repo_opensearch_version}" ]]; then
    echo "OpenSearch version mismatch in ${repo}: indexer='${OPENSEARCH_VERSION}', repo='${repo_opensearch_version}'"
    opensearch_mismatches=1
  fi
done <<< "${PLUGIN_VERSIONS}"

if [[ "${version_mismatches}" -ne 0 || "${opensearch_mismatches}" -ne 0 ]]; then
  echo "Compatibility check failed."
  exit 1
fi

echo "Compatibility check passed: all repositories match Wazuh Indexer version and OpenSearch version."
