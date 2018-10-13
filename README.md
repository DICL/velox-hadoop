VELOX-HADOOP
============

VELOXDFS Java library for HADOOP. 


CONFIGURE
=========


Make a file named `~/.velox-env.sh` which the content such as:

```sh
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export HADOOP_CLASSPATH="/home/vicente/.m2/repository/org/apache/hadoop/fs/velox/velox-hadoop/1.0/velox-hadoop-1.0.jar"
export HADOOP_CLASSPATH="/home/vicente/sandbox/lib/java/*:$HADOOP_CLASSPATH"
export HADOOP_CONF_DIR=/home/vicente/hadoop-etc
export JAVA_LIBRARY_PATH=/home/vicente/sandbox/lib
export HADOOP_HOME=/home/vicente/opt/hadoop-2.7.3
export PATH+=:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_SCRATCH=/scratch/vicente

```

INSTALL
=======

$ mvn install


RUN
===

Refer to the project `https://github.com/vicentebolea/hadoop-etc` for configuring hadoop for VeloxDFS
