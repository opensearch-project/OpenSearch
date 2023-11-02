#!/bin/bash
# Wazuh Copyright (C) 2023 Wazuh Inc. (License GPLv2)
# Wazuh - Indexer set rollover policy and templates

# Policy settings
MIN_SHARD_SIZE=${MIN_SHARD_SIZE:-25}
MIN_INDEX_AGE=${MIN_INDEX_AGE:-"7d"}
MIN_DOC_COUNT=${MIN_DOC_COUNT:-200000000}
ISM_INDEX_PATTERNS=${ISM_INDEX_PATTERNS:-'["wazuh-alerts-*", "wazuh-archives-*", "-wazuh-alerts-4.x-sample*"]'}
ISM_PRIORITY=${ISM_PRIORITY:-50}

POLICY_NAME="rollover_policy"

INDEXER_URL="https://localhost:9200"

# curl settings shortcuts
C_AUTH="-u admin:admin"

#########################################################################
# Creates the rollover_policy ISM policy.
# Globals:
#   MIN_SHARD_SIZE: The minimum shard size in GB.
#   MIN_INDEX_AGE: The minimum index age.
#   MIN_DOC_COUNT: The minimum document count.
#   ISM_INDEX_PATTERNS: The index patterns to apply the policy.
#   ISM_PRIORITY: The policy priority.
# Arguments:
#   None.
# Returns:
#   The rollover policy as a JSON string
#########################################################################
function generate_rollover_policy() {
  cat <<EOF
  {
    "policy": {
      "description": "Wazuh rollover and alias policy",
      "default_state": "active",
      "states": [
        {
          "name": "active",
          "actions": [
            {
              "rollover": {
                "min_primary_shard_size": "${MIN_SHARD_SIZE}gb",
                "min_index_age": "${MIN_INDEX_AGE}",
                "min_doc_count": "${MIN_DOC_COUNT}"
              }
            }
          ]
        }
      ],
      "ism_template": {
        "index_patterns": $ISM_INDEX_PATTERNS,
        "priority": "${ISM_PRIORITY}"
      }
    }
  }
EOF
}


#########################################################################
# Creates an index template with order 3 to set the rollover alias.
# Arguments:
#   - The alias name, a string. Also used as index pattern.
# Returns:
#   The index template as a JSON string.
#########################################################################
function generate_rollover_template() {
  cat <<EOF
  {
    "order": 3,
    "index_patterns": ["$1-*"],
    "settings": {
      "index.plugins.index_state_management.rollover_alias": "$1"
    }
  }
EOF
}


#########################################################################
# Loads the index templates for the rollover policy to the indexer.
#########################################################################
function load_templates() {
  # Note: the wazuh-template.json could also be loaded here.
  for alias in "${aliases[@]}"; do
    echo "TEMPLATES AND POLICIES - Uploading ${alias} template"
    generate_rollover_template "${alias}" | curl -s -k ${C_AUTH} \
      -X PUT "$INDEXER_URL/_template/${alias}-rollover" -o /dev/null \
      -H 'Content-Type: application/json' -d @-
  done
}


#########################################################################
# Uploads the rollover policy.
#   If the policy does not exist, the policy "${POLICY_NAME}" is created.
#   If the policy exists, but the rollover conditions are different, the
#   policy is updated.
# Arguments:
#   None.
#########################################################################
function upload_rollover_policy() {
  policy_exists=$(
    curl -s -k ${C_AUTH} \
    -X GET "$INDEXER_URL/_plugins/_ism/policies/${POLICY_NAME}" \
    -o /dev/null \
    -w "%{http_code}"
  )

  # Check if the ${POLICY_NAME} ISM policy was loaded (404 error if not found)
  if [[ $policy_exists == "404" ]]; then
    echo "TEMPLATES AND POLICIES - Uploading ${POLICY_NAME} ISM policy"
    generate_rollover_policy | curl -s -k ${C_AUTH} -o /dev/null \
      -X PUT "$INDEXER_URL/_plugins/_ism/policies/${POLICY_NAME}" \
      -H 'Content-Type: application/json' -d @-
  else
    if [[ $policy_exists == "200" ]]; then
      echo "TEMPLATES AND POLICIES - ${POLICY_NAME} policy already exists"
    else
      echo "TEMPLATES AND POLICIES - Error uploading ${POLICY_NAME} policy"
    fi
  fi
}


#########################################################################
# Check if an alias exists in the indexer.
# Arguments:
#   1. The alias to look for. String.
#########################################################################
function check_for_write_index() {
  curl -s -k ${C_AUTH} "$INDEXER_URL/_cat/aliases" | \
  grep -i "${1}" | \
  grep -i true | \
  awk '{print $2}'
}


#########################################################################
# Creates the settings for the aliased write index.
# Arguments:
#   1. The alias. String.
#########################################################################
function generate_write_index_alias() {
  cat << EOF
  {
    "aliases": {
      "$1": {
        "is_write_index": true
      }
    }
  }
EOF
}


#########################################################################
# Creates the initial aliased write index.
# Arguments:
#   1. The alias. String.
#########################################################################
function create_write_index() {
  curl -s -k ${C_AUTH} -o /dev/null \
    -X PUT "$INDEXER_URL/%3C${1}-4.x-%7Bnow%2Fd%7D-000001%3E?pretty" \
    -H 'Content-Type: application/json' -d "$(generate_write_index_alias "${1}")"
}


#########################################################################
# Creates the write indices for the aliases given as parameter.
# Arguments:
#   1. List of aliases to initialize.
#########################################################################
function create_indices() {
  echo "TEMPLATES AND POLICIES - Creating write indices"
  for alias in "${aliases[@]}"; do
    # Check if there are any write indices for the current alias
    write_index_exists=$(check_for_write_index "${alias}")

    # Create the write index if it does not exist
    if [[ -z $write_index_exists ]]; then
      create_write_index "${alias}"
    fi
  done
}


#########################################################################
# Main function.
#########################################################################
function main() {
  # The list should contain every alias which indices implement the
  # rollover policy
  aliases=("wazuh-alerts" "wazuh-archives")

  # Load the Wazuh Indexer templates
  load_templates

  # Upload the rollover policy
  upload_rollover_policy

  # Create the initial write indices
  create_indices "${aliases[@]}"
}

main "$@"