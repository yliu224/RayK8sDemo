FROM openjdk:17-jdk-slim

ENV SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin:/opt/spark/sbin \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

# Install Python and deps
RUN apt-get update && \
    apt-get install -y curl bash tini python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Download Spark prebuilt for Hadoop 3
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME



WORKDIR /opt/spark

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN python --version

COPY demo/ demo/
COPY main.py main.py