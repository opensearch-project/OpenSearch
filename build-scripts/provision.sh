#!/bin/bash

# Resolves core build dependencies and libraries for DEB distributions.
# Detects and configures architecture-specific tools for ARM64 (aarch64).

set -e

# Environment Detection
ARCH=$(uname -m)

echo "--- Starting Provisioning ---"
echo "Architecture Detected: $ARCH"

# DEB-BASED Logic
if command -v apt-get &> /dev/null; then
    echo "DEB-based system detected (apt-get found). Processing installation..."

    sudo apt-get update -y && sudo apt-get upgrade -y && sudo apt-get install -y curl build-essential
    sudo apt-get install -y debmake debhelper-compat
    sudo apt-get install -y libxrender1 libxtst6 libxi6
    sudo apt-get install -y libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
        libatspi2.0-dev libxcomposite-dev libxdamage1 libxfixes3 libxfixes-dev \
        libxrandr2 libgbm-dev libxkbcommon-x11-0 libpangocairo-1.0-0 \
        libcairo2 libcairo2-dev libnss3 libnspr4 libnspr4-dev jq
    sudo apt-get clean -y
fi

# Architecture logic (ARM64)
if [[ "$ARCH" == "aarch64" ]]; then
    echo "ARM64 detected: Installing architecture-specific tools..."

    # Install Maven, RPM, and CPIO
    sudo apt-get update
    sudo apt-get install -y maven rpm cpio

    if ! command -v docker &> /dev/null; then
        echo "Installing Docker..."

        sudo apt-get install -y docker.io
        sudo systemctl start docker
        sudo chmod 666 /var/run/docker.sock
    fi

fi

echo "--- Provisioning Finished ---"
