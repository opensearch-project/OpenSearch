#!/bin/bash

# Resolves core build dependencies and libraries for DEB distributions.
# Detects and configures architecture-specific tools for ARM64 (aarch64).

set -e

# Source shared retry utility
LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/lib" && pwd)"
source "${LIB_DIR}/retry.sh"

# Environment Detection
ARCH=$(uname -m)

echo "--- Starting Provisioning ---"
echo "Architecture Detected: $ARCH"

# DEB-BASED Logic
if command -v apt-get &> /dev/null; then
    echo "DEB-based system detected (apt-get found). Processing installation..."

    retry 3 10 sudo apt-get update -y && retry 3 10 sudo apt-get upgrade -y && retry 3 10 sudo apt-get install -y curl build-essential
    retry 3 10 sudo apt-get install -y debmake debhelper-compat
    retry 3 10 sudo apt-get install -y libxrender1 libxtst6 libxi6
    retry 3 10 sudo apt-get install -y libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
        libatspi2.0-dev libxcomposite-dev libxdamage1 libxfixes3 libxfixes-dev \
        libxrandr2 libgbm-dev libxkbcommon-x11-0 libpangocairo-1.0-0 \
        libcairo2 libcairo2-dev libnss3 libnspr4 libnspr4-dev jq
    sudo apt-get clean -y
fi

# Architecture logic (ARM64)
if [[ "$ARCH" == "aarch64" ]]; then
    echo "ARM64 detected: Installing architecture-specific tools..."

    # Install Maven, RPM, and CPIO
    retry 3 10 sudo apt-get update
    retry 3 10 sudo apt-get install -y maven rpm cpio
    retry 3 10 sudo apt-get update && retry 3 10 sudo apt-get install -y unzip
    retry 3 5 curl --connect-timeout 10 --max-time 120 "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
    unzip -q awscliv2.zip
    sudo ./aws/install --update
    
    if ! command -v docker &> /dev/null; then
        echo "Installing Docker..."

        retry 3 10 sudo apt-get install -y docker.io
        sudo systemctl start docker
        sudo chmod 666 /var/run/docker.sock
    fi

fi

echo "--- Provisioning Finished ---"
