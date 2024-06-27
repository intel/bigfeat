FROM centos:latest

ENV container docker

LABEL maintainer="bigfeat"

# copy Hive standalone package
COPY apache-hive-3.1.3-bin /opt/apache-hive-3.1.3-bin/

# copy Hadoop package
COPY hadoop-3.3.6 /opt/hadoop-3.3.6/

# copy Postgres or MySQL JDBC connector
COPY postgresql-42.6.0.jar /opt/apache-hive-3.1.3-bin/lib/

WORKDIR /install

RUN echo "proxy=$http_proxy" >> /etc/yum.conf
RUN cd /etc/yum.repos.d/
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
RUN yum update -y

# install Java 1.8 and clean cache
RUN yum install -y java-1.8.0-openjdk-devel \
  && yum clean all

# environment variables requested by Hive metastore
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV HADOOP_HOME=/opt/hadoop-3.3.6

# replace a library and add missing libraries
RUN rm -f /opt/apache-hive-3.1.3-bin/lib/guava-19.0.jar \
  && cp ${HADOOP_HOME}/share/hadoop/common/lib/guava-27.0-jre.jar /opt/apache-hive-3.1.3-bin/lib \
  && cp ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar /opt/apache-hive-3.1.3-bin/lib \
  && cp ${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar /opt/apache-hive-3.1.3-bin/lib

WORKDIR /opt/apache-hive-3.1.3-bin

# copy Hive metastore configuration file
COPY config/hive_config/hive-site.xml /opt/apache-hive-3.1.3-bin/conf/

# Hive metastore data folder
VOLUME ["/user/hive/warehouse"]

# create metastore backend tables and insert data. Replace postgres with mysql if MySQL backend used
RUN bin/schematool -initSchema -dbType postgres --verbose

WORKDIR /

RUN chmod +x "/opt/apache-hive-3.1.3-bin/bin/start-metastore"
CMD ["/opt/apache-hive-3.1.3-bin/bin/start-metastore"]
