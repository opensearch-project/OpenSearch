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

sleep 2

if kill -0 $1 2>/dev/null; then
    echo "Process $1 is still running"
else
    echo "Process $1 has terminated"
fi