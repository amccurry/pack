#!/bin/bash
set -e
if [ "$#" -ne 1 ]; then
  echo "Usage:"
  echo "${0} <volumename>"
fi
VOLUME_NAME="${1}"
curl -s localhost:4567/api/v1.0/volume/snapshot/${VOLUME_NAME} | jq -r '.'
