#!/bin/bash

# rpm_setup_environment.sh
# This script sets up the environment to perform smoke tests for the RPM distribution.
#
# Usage:
#   rpm_setup_environment.sh [--dry-run] <architecture> <run_id>
#
# Arguments:
#   architecture    [required] Target architecture: x64 or arm64.
#   run_id          [required] Unique run ID used for naming the instance.
# Options:
#   --dry-run       Show the command that would be run, but do not execute it.

set -euo pipefail

# Initialize variables
DRY_RUN=false
POSITIONAL_ARGS=()

# Parse arguments
for arg in "$@"; do
  case "$arg" in
    --dry-run)
      DRY_RUN=true
      ;;
    -h|--help)
      echo "Usage: $0 [--dry-run] <architecture> <run_id>"
      echo
      echo "Arguments:"
      echo "  architecture    Target architecture: x64 or arm64"
      echo "  run_id          Unique identifier to be used in the instance name"
      echo
      echo "Options:"
      echo "  --dry-run       Show the command that would be executed, without running it"
      exit 0
      ;;
    -*)
      echo "❌ Unknown option: $arg"
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$arg")
      ;;
  esac
done

# Restore positional arguments
set -- "${POSITIONAL_ARGS[@]}"

# Check that we have exactly two arguments left
if [[ $# -ne 2 ]]; then
    echo "❌ Error: Architecture and run_id are required."
    echo "Try '$0 --help' for usage."
    exit 1
fi

ARCH="$1"
RUN_ID="$2"

# Constants
INVENTORY_OUTPUT="/tmp/inventory.yaml"
TRACK_OUTPUT="/tmp/track.yaml"
WORKDIR="/tmp/indexer"
LABEL_TEAM="indexer"
TERMINATION_DATE="1d"
ALLOCATOR_SCRIPT="wazuh-automation/deployability/modules/allocation/main.py"

# Architecture-based settings
case "$ARCH" in
    x64)
        COMPOSITE_NAME="linux-centos-9-amd64"
        INSTANCE_NAME="indexer_amd_${RUN_ID}"
        ;;
    arm64)
        COMPOSITE_NAME="linux-centos-8-arm64"
        INSTANCE_NAME="indexer_arm_${RUN_ID}"
        ;;
    *)
        echo "❌ Error: Invalid architecture '$ARCH'. Valid options are: x64, arm64."
        exit 1
        ;;
esac

echo "🚀 Starting deployment for architecture: $ARCH, run_id: $RUN_ID"
echo "🔧 Instance name: $INSTANCE_NAME"
echo "📦 Composite name: $COMPOSITE_NAME"

# Build the command
CMD=(
    python3 "$ALLOCATOR_SCRIPT"
    --action create
    --provider aws
    --size large
    --composite-name "$COMPOSITE_NAME"
    --instance-name "$INSTANCE_NAME"
    --inventory-output "$INVENTORY_OUTPUT"
    --track-output "$TRACK_OUTPUT"
    --label-team "$LABEL_TEAM"
    --label-termination-date "$TERMINATION_DATE"
    --working-dir "$WORKDIR"
)

# Execute or simulate
if $DRY_RUN; then
    echo "🧪 Dry run mode enabled. The following command would be executed:"
    printf '%q ' "${CMD[@]}"
    echo
else
    "${CMD[@]}"
fi
