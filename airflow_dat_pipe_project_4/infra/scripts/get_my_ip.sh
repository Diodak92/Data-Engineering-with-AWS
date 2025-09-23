#!/usr/bin/env bash
# Returns your public IP in JSON format for Terraform external data
IP=$(curl -s ifconfig.me)
echo "{\"ip\": \"${IP}\"}"
