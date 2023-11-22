#!/bin/bash

# Wazuh-indexer securityadmin wrapper
# Copyright (C) 2022, Wazuh Inc.
#
# This program is a free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public
# License (version 2) as published by the FSF - Free Software
# Foundation.

CONFIG_PATH="/etc/wazuh-indexer"

if [ ! -d "${CONFIG_PATH}" ]; then
    echo "ERROR: it was not possible to find ${CONFIG_PATH}"
    exit 1
fi

CONFIG_FILE="${CONFIG_PATH}/opensearch.yml"

if [ ! -f "${CONFIG_FILE}" ]; then
    echo "ERROR: it was not possible to find ${CONFIG_FILE}"
    exit 1
fi

INSTALL_PATH="/usr/share/wazuh-indexer"

if [ ! -d "${INSTALL_PATH}" ]; then
        echo "ERROR: it was not possible to find ${INSTALL_PATH}"
        exit 1
fi

HOST=""
OPTIONS="-icl -nhnv"
WAZUH_INDEXER_ROOT_CA="$(cat ${CONFIG_FILE} 2>&1 | grep http.pemtrustedcas | sed 's/.*: //' | tr -d "[\"\']")"
WAZUH_INDEXER_ADMIN_PATH="$(dirname "${WAZUH_INDEXER_ROOT_CA}" 2>&1)"
SECURITY_PATH="${INSTALL_PATH}/plugins/opensearch-security"
SECURITY_CONFIG_PATH="${CONFIG_PATH}/opensearch-security"

# -----------------------------------------------------------------------------

trap ctrl_c INT

clean(){

    exit_code=$1
    indexer_process_id=$(pgrep -f wazuh-indexer -c)
    if [ "${indexer_process_id}" -gt 1 ]; then
        pkill -n -f wazuh-indexer
    fi
    exit "${exit_code}"
    
}

ctrl_c() {
    clean 1
}

# -----------------------------------------------------------------------------

getNetworkHost() {

    HOST=$(grep -hr "network.host:" "${CONFIG_FILE}" 2>&1)
    NH="network.host: "
    HOST="${HOST//$NH}"
    HOST=$(echo "${HOST}" | tr -d "[\"\']")

    isIP=$(echo "${HOST}" | grep -P "^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$")
    isDNS=$(echo "${HOST}" | grep -P "^[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9](?:\.[a-zA-Z]{2,})+$")

    # Allow to find ip with an interface
    if [ -z "${isIP}" ] && [ -z "${isDNS}" ]; then
        interface="${HOST//_}"
        HOST=$(ip -o -4 addr list "${interface}" | awk '{print $4}' | cut -d/ -f1) 
    fi

    if [ "${HOST}" = "0.0.0.0" ]; then
        HOST="127.0.0.1"
    fi

    if [ -z "${HOST}" ]; then
        echo "ERROR: network host not valid, check ${CONFIG_FILE}"
        exit 1
    fi

}

# -----------------------------------------------------------------------------
getPort() {

    PORT=$(grep -hr 'transport.tcp.port' "${CONFIG_FILE}" 2>&1)
    if [ "${PORT}" ]; then
        PORT=$(echo "${PORT}" | cut -d' ' -f2 | cut -d'-' -f1)
    else
        PORT="9200"
    fi
    PORT=$(echo "${PORT}" | tr -d "[\"\']")

}
# -----------------------------------------------------------------------------

securityadmin() {

    if [ ! -d "${SECURITY_PATH}" ]; then
        echo "ERROR: it was not possible to find ${SECURITY_PATH}"
        exit 1
    elif [ ! -d "${INSTALL_PATH}/jdk" ]; then
        echo "ERROR: it was not possible to find ${INSTALL_PATH}/jdk"
        exit 1
    fi

    if [ -f "${WAZUH_INDEXER_ADMIN_PATH}/admin.pem" ] && [ -f "${WAZUH_INDEXER_ADMIN_PATH}/admin-key.pem" ] && [ -f "${WAZUH_INDEXER_ROOT_CA}" ]; then
        OPENSEARCH_CONF_DIR="${CONFIG_PATH}" JAVA_HOME="${INSTALL_PATH}/jdk" runuser wazuh-indexer --shell="/bin/bash" --command="${SECURITY_PATH}/tools/securityadmin.sh -cd ${SECURITY_CONFIG_PATH} -cacert ${WAZUH_INDEXER_ROOT_CA} -cert ${WAZUH_INDEXER_ADMIN_PATH}/admin.pem -key ${WAZUH_INDEXER_ADMIN_PATH}/admin-key.pem -h ${HOST} -p ${PORT} ${OPTIONS}"
    else
        echo "ERROR: this tool try to find admin.pem and admin-key.pem in ${WAZUH_INDEXER_ADMIN_PATH} but it couldn't. In this case, you must run manually the Indexer security initializer by running the command: JAVA_HOME="/usr/share/wazuh-indexer/jdk" runuser wazuh-indexer --shell="/bin/bash" --command="/usr/share/wazuh-indexer/plugins/opensearch-security/tools/securityadmin.sh -cd /etc/wazuh-indexer/opensearch-security -cacert /path/to/root-ca.pem -cert /path/to/admin.pem -key /path/to/admin-key.pem -h ${HOST} -p ${PORT} ${OPTIONS}" replacing /path/to/ by your certificates path."
        exit 1
    fi

}

help() {
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "    -ho, --host <host>    [Optional] Target IP or DNS to configure security."
    echo "    --port <port>         [Optional] wazuh-indexer security port."
    echo "    --options <options>   [Optional] Custom securityadmin options."
    echo "    -h, --help            Show this help."
    echo
    exit "$1"
}


main() {

    getNetworkHost
    getPort

    while [ -n "$1" ]
    do
        case "$1" in
        "-h"|"--help")
            help 0
            ;;
        "-ho"|"--host")
            if [ -n "$2" ]; then
                HOST="$2"
                HOST=$(echo "${HOST}" | tr -d "[\"\']")
                isIP=$(echo "${2}" | grep -P "^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$")
                isDNS=$(echo "${2}" | grep -P "^[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9](?:\.[a-zA-Z]{2,})+$")
                if [[ -z "${isIP}" ]] &&  [[ -z "${isDNS}" ]]; then
                    echo "The given information does not match with an IP address or a DNS."
                    exit 1
                fi
                shift 2
            else
                help 1
            fi
            ;;
        "--port")
            if [ -n "$2" ]; then
                PORT="$2"
                PORT=$(echo "${PORT}" | tr -d "[\"\']")
                if [[ -z $(echo "${2}" | grep -P "^([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$") ]]; then
                    echo "The given information does not match with a valid PORT number."
                    exit 1
                fi
                shift 2
            else
                help 1
            fi
            ;;
        "--options")
            if [ -n "$2" ]; then
                OPTIONS="$2"
                shift 2
            else
                help 1
            fi
            ;;
        *)
            help 1
        esac
    done

    securityadmin

}

main "$@"
