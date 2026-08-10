#!/bin/bash

# Check whether enp6s0f0 exists and is UP
if ip link show enp6s0f0 &> /dev/null && \
   ip link show enp6s0f0 | grep -q 'state UP'
then
    wondershaper -a enp6s0f0 -d 512000 -u 512000
    exit 0
fi

# Check whether enp6s0f1 exists and is UP
if ip link show enp6s0f1 &> /dev/null && \
   ip link show enp6s0f1 | grep -q 'state UP'
then
    wondershaper -a enp6s0f1 -d 512000 -u 512000
    exit 0
fi

# Error out if neither interface is usable
echo "Error: No active interface found!" >&2
exit 1