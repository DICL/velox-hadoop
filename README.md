VELOX-HADOOP
============

VELOXDFS Java library for HADOOP. 


CONFIGURE
=========


Make a file named `~/.velox-env.sh` which the content such as:

```sh
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export HADOOP_CLASSPATH="$HOME/.m2/repository/org/dicl/velox/velox-hadoop/1.0/velox-hadoop-1.0.jar"
export HADOOP_CLASSPATH="$HOME/sandbox/lib/java/*:$HADOOP_CLASSPATH"
export HADOOP_CONF_DIR=$HOME/hadoop-etc
export JAVA_LIBRARY_PATH=$HOME/sandbox/lib
export HADOOP_HOME=$HOME/opt/hadoop-2.7.3
export PATH+=:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_SCRATCH=/scratch/vicente

```

INSTALL
=======

    $ gradle publishToMavenLocal

    # Or if you do not have gradle installed
    $ ./gradlew publishToMavenLocal


RUN
===

Refer to the project `https://github.com/vicentebolea/hadoop-etc` for configuring hadoop for VeloxDFS

One example way of running it will be for Aggregate WordCount:

```sh
LIBJARS=~/.m2/repository/org/dicl/velox/velox-hadoop/1.0/velox-hadoop-1.0.jar

# Aggregation Wordcount
time hadoop jar $LIBJARS AggregateWordCount -libjars $LIBJARS /text_100GB.dat /output.`date +%N` 1 textinputformat

# Simple WordCount
time hadoop jar $LIBJARS wordcount -libjars $LIBJARS /text_100GB.dat /output.`date +%N`
```
