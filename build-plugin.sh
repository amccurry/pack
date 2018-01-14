#!/bin/bash
set -x
set -e
rm -rf docker-pack-plugin/rootfs
docker build -t pack-rootfsimage .
id=$(docker create pack-rootfsimage true)
mkdir -p docker-pack-plugin/rootfs
cd docker-pack-plugin/rootfs
docker export "$id" | tar -x
docker rm -vf "$id"
docker rmi pack-rootfsimage
