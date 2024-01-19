#!/bin/bash

# Provision script for assembly of DEB packages

# Install necessary packages
apt-get update -y && apt-get upgrade -y && apt-get install -y curl build-essential &&
   apt-get install -y debmake debhelper-compat &&
   apt-get install -y libxrender1 libxtst6 libasound2 libxi6 libgconf-2-4 &&
   apt-get install -y libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libatspi2.0-dev libxcomposite-dev libxdamage1 libxfixes3 libxfixes-dev libxrandr2 libgbm-dev libxkbcommon-x11-0 libpangocairo-1.0-0 libcairo2 libcairo2-dev libnss3 libnspr4 libnspr4-dev freeglut3 &&
   apt-get clean -y
