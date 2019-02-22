#!/bin/bash
set -e
set -x

export HDFS_CONF_DIR="/etc/hadoop/conf"
export PACK_HDFS_PATH="/pack/volumes"
export PACK_HDFS_USER="pack"
export PACK_LOCAL="/var/lib/pack"
export PACK_LOG4J_CONFIG="/home/amccurry/Development/git-projects/pack/pack-block/src/test/resources/log4j.xml"
export PACK_ZOOKEEPER_CONNECTION_STR="centos-02,centos-03,centos-04/pack"
export PACK_PARCEL_PATH="/home/amccurry/Development/git-projects/pack/pack-assemble/target/PACK-18*/PACK-18*/"
export PACK_HDFS_SUPER_USER="hdfs"

sudo -E $PACK_PARCEL_PATH/bin/run.sh compaction
