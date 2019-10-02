#!/bin/bash
set -e
curl -s localhost:4567/api/v1.0/volume | jq -r '.'
