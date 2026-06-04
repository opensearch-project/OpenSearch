#!/bin/sh

if [ $# -ne 1 ]; then
    echo "Usage: $0 <PID>"
    exit 1
fi

if kill -15 $1 2>/dev/null; then
    echo "SIGTERM signal sent to process $1"
else
    echo "Failed to send SIGTERM to process $1"
fi