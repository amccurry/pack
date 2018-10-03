#!/bin/bash

for I in {0..5}; do
  docker run -d --rm -v test${I}:/test -v /root/pack/basic-md5-concurrent-test.sh:/basic-md5-concurrent-test.sh centos /basic-md5-concurrent-test.sh
done
