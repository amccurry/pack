FROM centos:7
RUN yum -y update && \
  yum -y install wget && \
  wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" \
"http://download.oracle.com/otn-pub/java/jdk/8u111-b14/jdk-8u111-linux-x64.rpm" && \
  yum -y install jdk-8u111-linux-x64.rpm && \
  rm -f jdk-8u111-linux-x64.rpm && \
  yum -y clean all
COPY pack-block/target/pack-block-1.0-SNAPSHOT/lib/ /pack/lib/
COPY bin/ /pack/bin/
ENTRYPOINT ["/pack/bin/entrypoint.sh", "/pack/bin/run.sh"]
