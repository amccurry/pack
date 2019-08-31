#!/bin/bash
set -e
docker run -v "$PWD:/data" thrift thrift -out /data/src/main/java --gen java /data/src/main/resources/service.thrift
sudo chown -R apm:apm src/main/java/pack
