#!/bin/bash

# Function to get OpenSearch PID
get_opensearch_pid() {
    pid=$(systemctl show -p MainPID opensearch | cut -d'=' -f2)
    if [ "$pid" -eq 0 ]; then
        echo "OpenSearch is not running" >&2
        exit 1
    fi
    echo "$pid"
}

# Function to check service status
check_service_status() {
    status=$(systemctl is-active opensearch)
    if [ "$status" != "active" ]; then
        echo "Service status check failed. Expected 'active', got '$status'" >&2
        exit 1
    fi
}

# Main test
main() {
    # Get OpenSearch PID
    opensearch_pid=$(get_opensearch_pid)
    
    # Run terminate script
    ./terminate.sh "$opensearch_pid"
    
    # Wait for potential termination
    sleep 2
    
    # Check service status
    check_service_status
    
    echo "Test passed: OpenSearch process cannot be terminated"
}

main "$@"