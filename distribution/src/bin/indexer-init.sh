#!/bin/bash
# Wazuh Copyright (C) 2023 Wazuh Inc. (License GPLv2)
# Wazuh - indexer initialization script

INSTALL_PATH="/usr/share/wazuh-indexer"
BIN_PATH="${INSTALL_PATH}/bin"


#########################################################################
# Parse arguments for security init script.
#########################################################################
function parse_security_args() {
    security_args=()

    while [ -n "$1" ]; do
        case "$1" in
        "-h" | "--help")
            security_args+=("${1}")
            shift
            ;;
        "-ho" | "--host")
            if [ -n "$2" ]; then
                security_args+=("${1}" "${2}")
                shift 2
            fi
            ;;
        "--port")
            if [ -n "$2" ]; then
                security_args+=("${1}" "${2}")
                shift 2
            fi
            ;;
        "--options")
            if [ -n "$2" ]; then
                security_args+=("${1}" "${2}")
                shift 2
            fi
            ;;
        *)
            shift
            ;;
        esac
    done
}


#########################################################################
# Run the security init script.
#########################################################################
function run_security_init() {
    echo "Executing Wazuh indexer security init script..."
    parse_security_args "$@"
    /bin/bash "${BIN_PATH}/indexer-security-init.sh" "${security_args[@]}"
}


#########################################################################
# Parse arguments for ISM init script.
#########################################################################
function parse_ism_args() {
    ism_args=()

    while [ -n "${1}" ]; do
        case "${1}" in
        "-a" | "--min-index-age")
            if [ -n "${2}" ]; then
                ism_args+=("${1}" "${2}")
                shift 2
            fi
            ;;
        "-d" | "--min-doc-count")
            if [ -n "${2}" ]; then
                ism_args+=("${1}" "${2}")
                shift 2
            fi
            ;;
        "-h" | "--help")
            ism_args+=("${1}")
            shift
            ;;
        "-i" | "--indexer-hostname")
            if [ -n "${2}" ]; then
                ism_args+=("${1}" "${2}")
                shift 2
            fi
            ;;
        "-p" | "--indexer-password")
            if [ -n "${2}" ]; then
                ism_args+=("${1}" "${2}")
                shift 2
            fi
            ;;
        "-s" | "--min-shard-size")
            if [ -n "${2}" ]; then
                ism_args+=("${1}" "${2}")
                shift 2
            fi
            ;;
        "-P" | "--priority")
            if [ -n "${2}" ]; then
                ism_args+=("${1}" "${2}")
                shift 2
            fi
            ;;
        "-v" | "--verbose")
            ism_args+=("${1}")
            shift
            ;;
        *)
            shift
            ;;
        esac
    done
}


#########################################################################
# Run the ISM init script.
#########################################################################
function run_ism_init() {
    echo "Executing Wazuh indexer ISM init script..."
    parse_ism_args "$@"
    /bin/bash "${BIN_PATH}/indexer-ism-init.sh" "${ism_args[@]}";
}


#########################################################################
# Main function.
#########################################################################
function main() {
    # If run_security_init returns 0, then run_ism_init
    if run_security_init "$@" -gt 0; then
        run_ism_init "$@"
    fi
}


main "$@"
