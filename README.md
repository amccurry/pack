# pack
Simple docker volume driver to pack the contents of a mount an upload to hdfs cluster.
~~~~
docker run -d \
  --name pack \
  --net host \
  --privileged \
  -e pack.hdfs.path="<hdfs path>" \
  -e pack.hdfs.user=pack \
  -e pack.scope=global \
  -v /var/lib/pack:/var/lib/pack \
  -v /etc/docker:/etc/docker \
  -v /var/run/docker.sock:/var/run/docker.sock \
  <image name>
~~~~
