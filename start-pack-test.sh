#!/bin/bash
set -e
set -x

rm -rf ./hadoop-conf

docker cp hadoop-docker:/usr/local/hadoop/etc/hadoop ./hadoop-conf

CWD=$(pwd -P)

cd pack-assemble/target/PACK-*.dir/*

PACK_PARCEL=$(pwd -P)

cd $CWD

#-e HADOOP_USER_NAME=hdfs \
#-e PACK_HDFS_USER=hdfs \

docker run --name pack --hostname pack --privileged --rm -it --link hadoop-docker \
-e PACK_HDFS_PATH=/pack \
-e PACK_PARCEL_PATH=/pack \
-e HDFS_CONF_DIR=/etc/hadoop/conf \
-e PACK_HDFS_USER=root \
-e PACK_LOCAL=/var/lib/pack \
-e TEST_ZK=true \
-v $CWD/hadoop-conf:/etc/hadoop/conf \
-v $PACK_PARCEL:/pack \
-v /var/run/docker.sock:/run/docker.sock \
-v /var/lib/pack:/var/lib/pack \
-v /run/docker/plugins:/run/docker/plugins \
-v ${PWD}/log:/var/log/pack \
java
