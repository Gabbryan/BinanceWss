FROM python:3.10-bullseye as spark-base

#Get home compose declaration
ARG SPARK_HOME
ARG HADOOP_HOME

#Target Spark and Hadoop Version
ENV SPARK_VERSION=3.5.1 \
HADOOP_VERSION=3.3.4 \
PYTHONHASHSEED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-libs \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setup the directories for our Spark and Hadoop installations
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download and install Spark
RUN curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o apache-spark.tgz \
 && tar xvzf apache-spark.tgz --directory ${SPARK_HOME} --strip-components 1 \
 && rm -rf apache-spark.tgz

# Download and install Hadoop
RUN curl -L https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -o hadoop-bin.tar.gz && \
    file hadoop-bin.tar.gz && \
    tar zxf hadoop-bin.tar.gz --directory ${HADOOP_HOME} --strip-components=1 || (echo "Failed to extract, here's the content:" && cat hadoop-bin.tar.gz) && \
    rm hadoop-bin.tar.gz

 RUN ls -l ${HADOOP_HOME}


# Ensure the Hadoop configuration directory exists
RUN mkdir -p ${HADOOP_HOME}/etc/hadoop
RUN ls -l ${HADOOP_HOME}/etc/hadoop
# Now append to the configuration file
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh


FROM spark-base as pyspark

# Install python deps
COPY requirements.txt .


RUN  python3 -m pip install --upgrade pip && \
 python3 -m pip install -r requirements.txt --no-cache-dir

# Set JAVA_HOME environment variable
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

# Add the Spark and Hadoop bin and sbin to the PATH variable.
# Also add $JAVA_HOME/bin to the PATH
ENV PATH="$SPARK_HOME/sbin:/opt/spark/bin:${PATH}"
ENV PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:${PATH}"
ENV PATH="${PATH}:${JAVA_HOME}/bin"

# Setup Spark related environment variables
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
ENV HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"

# Add Hadoop native library path to the dynamic link library path
ENV LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:${LD_LIBRARY_PATH}"

# Set user for HDFS and Yarn (for production probably not smart to put root)
ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

# Add JAVA_HOME to haddop-env.sh
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"

# COPY confiles (Set on volume)
COPY conf/spark-defaults.conf "$SPARK_HOME/conf"
COPY conf/yarn/*.xml "$HADOOP_HOME/etc/hadoop/"

# Make the binaries and scripts executable and set the PYTHONPATH environment variable
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

#Setup SSH
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
  chmod 600 ~/.ssh/authorized_keys
COPY conf/ssh_config ~/.ssh/config

# Copy entrypoint script to start cluster
COPY entrypoint.sh .
COPY ../../../../cores/aggTrades/live/transformation/src .

RUN chmod +x ./entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
