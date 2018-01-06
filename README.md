# pack

Simple docker volume driver to pack the contents of a mount and store in a HDFS cluster.  This driver does NOT make incremental changes to HDFS.  After a container exits pack creates a tar of the data in the volume and stores the tar in HDFS.  When the volume is mounted the tar is read from HDFS and extracted on the local host machine for the container to mount.  The tar maintains the permissions, group, and owner information between mounts.

## Building pack as a Docker container.
~~~~
docker build -t pack .
~~~~

## Running pack as a Docker container.
~~~~
docker run -d \
  --name pack \
  --cap-add SYS_ADMIN \
  --device /dev/fuse \
  -e PACK_ZOOKEEPER_CONNECTION_STR="<zk>/pack" \
  -e HDFS_CONF_DIR="/pack/hadoop-conf" \
  -v <hdfs config>:/pack/hadoop-conf \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /var/run/docker/plugins/:/var/run/docker/plugins/ \
  pack <[pack|compactor]>
~~~~

~~~~
docker run -it --rm \
  --name pack \
  --cap-add SYS_ADMIN \
  --device /dev/fuse \
  -e TEST_ZK=true \
  -e TEST_HDFS=true \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /var/run/docker/plugins/:/var/run/docker/plugins/ \
  pack pack
~~~~
