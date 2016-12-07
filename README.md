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
  --net host \
  --privileged \
  -e pack.hdfs.path="<hdfs path>" \
  -e pack.hdfs.user=pack \
  -e pack.scope=global \
  -v <hdfs config>:/pack/hadoop-conf \
  -v /var/lib/pack:/var/lib/pack \
  -v /etc/docker:/etc/docker \
  -v /var/run/docker.sock:/var/run/docker.sock \
  pack
~~~~
