#!/bin/bash

set -e
set -u

# Function to display usage information
show_usage() {
  echo "Usage: $0 <ECS_VERSION> <INDEXER_SRC> <MODULE> [--upload <URL>]"
  echo "  * ECS_VERSION: ECS version to generate mappings for"
  echo "  * INDEXER_SRC: Path to the wazuh-indexer repository"
  echo "  * MODULE: Module to generate mappings for"
  echo "  * --upload <URL>: Upload generated index template to the OpenSearch cluster. Defaults to https://localhost:9200"
  echo "Example: $0 v8.10.0 ~/wazuh-indexer vulnerability-detector --upload https://indexer:9200"
}

# Function to remove multi-fields from the generated index template
remove_multi_fields() {
  local IN_FILE="$1"
  local OUT_FILE="$2"

  jq 'del(
    .mappings.properties.host.properties.os.properties.full.fields,
    .mappings.properties.host.properties.os.properties.name.fields,
    .mappings.properties.vulnerability.properties.description.fields
  )' "$IN_FILE" > "$OUT_FILE"
}


# Function to generate mappings
generate_mappings() {
  local IN_FILES_DIR="$INDEXER_SRC/ecs/$MODULE/fields"
  local OUT_DIR="$INDEXER_SRC/ecs/$MODULE/mappings/$ECS_VERSION"

  # Ensure the output directory exists
  mkdir -p "$OUT_DIR" || exit 1

  # Generate mappings
  python scripts/generator.py --strict --ref "$ECS_VERSION" \
    --include "$IN_FILES_DIR/custom/" \
    --subset "$IN_FILES_DIR/subset.yml" \
    --template-settings "$IN_FILES_DIR/template-settings.json" \
    --template-settings-legacy "$IN_FILES_DIR/template-settings-legacy.json" \
    --mapping-settings "$IN_FILES_DIR/mapping-settings.json" \
    --out "$OUT_DIR" || exit 1

  # Replace "match_only_text" type (not supported by OpenSearch) with "text"
  echo "Replacing \"match_only_text\" type with \"text\""
  find "$OUT_DIR" -type f -exec sed -i 's/match_only_text/text/g' {} \;

  local IN_FILE="$OUT_DIR/generated/elasticsearch/legacy/template.json"
  local OUT_FILE="$OUT_DIR/generated/elasticsearch/legacy/template-tmp.json"

  # Delete the "tags" field from the index template
  echo "Deleting the \"tags\" field from the index template"
  jq 'del(.mappings.properties.tags)' "$IN_FILE" > "$OUT_FILE"
  mv "$OUT_FILE" "$IN_FILE"

  # Remove multi-fields from the generated index template
  echo "Removing multi-fields from the index template"
  remove_multi_fields "$IN_FILE" "$OUT_FILE"
  mv "$OUT_FILE" "$IN_FILE"

  # Transform legacy index template for OpenSearch compatibility
  cat "$IN_FILE" | jq '{
    "index_patterns": .index_patterns,
    "priority": .order,
    "template": {
      "settings": .settings,
      "mappings": .mappings
    }
  }' >"$OUT_DIR/generated/elasticsearch/legacy/opensearch-template.json"

  # Check if the --upload flag has been provided
  if [ "$UPLOAD" == "--upload" ]; then
    upload_mappings "$OUT_DIR" "$URL" || exit 1
  fi

  echo "Mappings saved to $OUT_DIR"
}

# Function to upload generated composable index template to the OpenSearch cluster
upload_mappings() {
  local OUT_DIR="$1"
  local URL="$2"

  echo "Uploading index template to the OpenSearch cluster"
  for file in "$OUT_DIR/generated/elasticsearch/composable/component"/*.json; do
    component_name=$(basename "$file" .json)
    echo "Uploading $component_name"
    curl -u admin:admin -X PUT "$URL/_component_template/$component_name?pretty" -H 'Content-Type: application/json' -d@"$file" || exit 1
  done
}

# Check if the minimum required arguments have been provided
if [ $# -lt 3 ]; then
  show_usage
  exit 1
fi

# Parse command line arguments
ECS_VERSION="$1"
INDEXER_SRC="$2"
MODULE="$3"
UPLOAD="${4:-false}"
URL="${5:-https://localhost:9200}"

# Generate mappings
generate_mappings "$ECS_VERSION" "$INDEXER_SRC" "$MODULE" "$UPLOAD" "$URL"
