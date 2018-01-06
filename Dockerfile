FROM centos:7
RUN yum -y update && \
  yum -y install wget sudo && \
  wget \
    --no-cookies \
    --no-check-certificate \
    --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" \
    "http://download.oracle.com/otn-pub/java/jdk/8u151-b12/e758a0de34e24606bca991d704f6dcbf/jdk-8u151-linux-x64.rpm" && \
  yum -y install jdk-8u151-linux-x64.rpm fuse && \
  rm -f jdk-8u151-linux-x64.rpm && \
  yum -y clean all

RUN sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo

RUN yum -y install fuse-libs xfsprogs util-linux e2fsprogs docker-ce

ENV PACK_LOCAL="/var/lib/pack"
ENV PACK_LOG="/var/log/pack"
ENV PACK_SCOPE="global"
ENV PACK_HDFS_PATH="/user/pack/volumes"
ENV PACK_PARCEL_PATH="/pack"
ENV PACK_NOHUP_PROCESS=false
ENV TINI_VERSION v0.16.1

ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

VOLUME [ "/var/lib/pack", /var/log/pack ]

ARG PACK_VERSION
COPY pack-assemble/target/PACK-${PACK_VERSION}.dir/PACK-${PACK_VERSION}/ /pack/
RUN mkdir -p /run/docker/plugins /var/lib/pack

ENTRYPOINT ["/tini", "--", "/pack/bin/run.sh"]
