#!/bin/bash
# Wazuh Copyright (C) 2023 Wazuh Inc. (License GPLv2)
# Wazuh - Indexer set rollover policy and templates

# Policy settings
MIN_SHARD_SIZE="25"
MIN_INDEX_AGE="7d"
MIN_DOC_COUNT="200000000"
ISM_INDEX_PATTERNS='["wazuh-alerts-*", "wazuh-archives-*", "-wazuh-alerts-4.x-sample*"]'
ISM_PRIORITY="50"
INDEXER_PASSWORD="admin"
INDEXER_HOSTNAME="localhost"

POLICY_NAME="rollover_policy"

INDEXER_URL="https://${INDEXER_HOSTNAME}:9200"

# curl settings shortcuts
C_AUTH="-u admin:${INDEXER_PASSWORD}"

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
    cat <<-EOF
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
                    "index_patterns": ${ISM_INDEX_PATTERNS},
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
    cat <<-EOF
        {
            "order": 3,
            "index_patterns": ["${1}-*"],
            "settings": {
                "index.plugins.index_state_management.rollover_alias": "${1}"
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
        generate_rollover_template "${alias}" |
            if ! curl -s -k ${C_AUTH} \
                -X PUT "${INDEXER_URL}/_template/${alias}-rollover" -o /dev/null \
                -H 'Content-Type: application/json' -d @-; then
                echo "Error uploading ${alias} template"
                return 1
            else
                echo "${alias} template uploaded"
            fi
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
            -X GET "${INDEXER_URL}/_plugins/_ism/policies/${POLICY_NAME}" \
            -o /dev/null \
            -w "%{http_code}"
    )

    # Check if the ${POLICY_NAME} ISM policy was loaded (404 error if not found)
    if [[ "${policy_exists}" == "404" ]]; then
        if ! curl -s -k ${C_AUTH} -o /dev/null \
            -X PUT "${INDEXER_URL}/_plugins/_ism/policies/${POLICY_NAME}" \
            -H 'Content-Type: application/json' \
            -d "$(generate_rollover_policy)"; then
            echo "Error uploading ${POLICY_NAME} policy"
            return 1
        else
            echo "${POLICY_NAME} policy uploaded"
        fi
    else
        if [[ "${policy_exists}" == "200" ]]; then
            echo "${POLICY_NAME} policy already exists"
        else
            echo "Error checking if ${POLICY_NAME} exists"
            return 1
        fi
    fi
}

#########################################################################
# Check if an alias exists in the indexer.
# Arguments:
#   1. The alias to look for. String.
#########################################################################
function check_for_write_index() {
    curl -s -k ${C_AUTH} "${INDEXER_URL}/_cat/aliases" |
        grep -i "${1}" |
        grep -i true |
        awk '{print $2}'
}

#########################################################################
# Creates the settings for the aliased write index.
# Arguments:
#   1. The alias. String.
#########################################################################
function generate_write_index_alias() {
    cat <<-EOF
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
    if ! curl -s -k ${C_AUTH} -o /dev/null \
        -X PUT "$INDEXER_URL/%3C${1}-4.x-%7Bnow%2Fd%7D-000001%3E?pretty" \
        -H 'Content-Type: application/json' \
        -d "$(generate_write_index_alias "${1}")"; then
        echo "Error creating ${1} write index"
        exit 1
    else
        echo "${1} write index created"
    fi
}

#########################################################################
# Creates the write indices for the aliases given as parameter.
# Arguments:
#   1. List of aliases to initialize.
#########################################################################
function create_indices() {
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
# Shows usage help.
#########################################################################
function show_help() {
    echo -e ""
    echo -e "NAME"
    echo -e "        indexer-ism-init.sh - Manages the Index State Management plugin for Wazuh indexer index rollovers policies."
    echo -e ""
    echo -e "SYNOPSIS"
    echo -e "        indexer-ism-init.sh [OPTIONS]"
    echo -e ""
    echo -e "DESCRIPTION"
    echo -e "        -a,  --min-index-age <index-age>"
    echo -e "                Set the minimum index age. By default 7d."
    echo -e ""
    echo -e "        -d, --min-doc-count <doc-count>"
    echo -e "                Set the minimum document count. By default 200000000."
    echo -e ""
    echo -e "        -h,  --help"
    echo -e "                Shows help."
    echo -e ""
    echo -e "        -i, --indexer-hostname <hostname>"
    echo -e "                Specifies the Wazuh indexer hostname or IP."
    echo -e ""
    echo -e "        -p, --indexer-password <password>"
    echo -e "                Specifies the Wazuh indexer admin user password."
    echo -e ""
    echo -e "        -P, --priority <priority>"
    echo -e "                Specifies the policy's priority."
    echo -e ""
    echo -e "        -s, --min-shard-size <shard-size>"
    echo -e "                Set the minimum shard size in GB. By default 25."
    echo -e ""
    echo -e "        -v, --verbose"
    echo -e "                Set verbose mode. Prints more information."
    echo -e ""
    exit 1
}

#########################################################################
# Main function.
#########################################################################
function main() {
    # The list should contain every alias which indices implement the
    # rollover policy
    aliases=("wazuh-alerts" "wazuh-archives")

    while [ -n "${1}" ]; do
        case "${1}" in
        "-a" | "--min-index-age")
            if [ -z "${2}" ]; then
                echo "Error on arguments. Probably missing <index-age> after -a|--min-index-age"
                show_help
            else
                MIN_INDEX_AGE="${2}"
                shift 2
            fi
            ;;
        "-d" | "--min-doc-count")
            if [ -z "${2}" ]; then
                echo "Error on arguments. Probably missing <doc-count> after -d|--min-doc-count"
                show_help
            else
                MIN_DOC_COUNT="${2}"
                shift 2
            fi
            ;;
        "-h" | "--help")
            show_help
            ;;
        "-i" | "--indexer-hostname")
            if [ -z "${2}" ]; then
                echo "Error on arguments. Probably missing <hostname> after -i|--indexer-hostname"
                show_help
            else
                INDEXER_HOSTNAME="${2}"
                INDEXER_URL="https://${INDEXER_HOSTNAME}:9200"
                shift 2
            fi
            ;;
        "-p" | "--indexer-password")
            if [ -z "${2}" ]; then
                echo "Error on arguments. Probably missing <password> after -p|--indexer-password"
                show_help
            else
                INDEXER_PASSWORD="${2}"
                C_AUTH="-u admin:${INDEXER_PASSWORD}"
                shift 2
            fi
            ;;
        "-s" | "--min-shard-size")
            if [ -z "${2}" ]; then
                echo "Error on arguments. Probably missing <shard-size> after -s|--min-shard-size"
                show_help
            else
                MIN_SHARD_SIZE="${2}"
                shift 2
            fi
            ;;
        "-P" | "--priority")
            if [ -z "${2}" ]; then
                echo "Error on arguments. Probably missing <priority> after -P|--priority"
                show_help
            else
                ISM_PRIORITY="${2}"
                shift 2
            fi
            ;;
        "-v" | "--verbose")
            set -x
            shift
            ;;
        *)
            echo "Unknow option: ${1}"
            show_help
            ;;
        esac
    done

    # Load the Wazuh Indexer templates
    # Upload the rollover policy
    # Create the initial write indices
    if load_templates && upload_rollover_policy && create_indices "${aliases[@]}"; then
        echo "Indexer ISM initialization finished successfully"
    else
        echo "Indexer ISM initialization failed"
        exit 1
    fi
}

main "$@"
