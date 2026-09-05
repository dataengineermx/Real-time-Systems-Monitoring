FROM flink:2.3.0-scala_2.12-java17

# ---------------------------------------------------------
# Python + PyFlink
# ---------------------------------------------------------

USER root

RUN apt-get update -y \
    && apt-get install -y python3 python3-pip python3-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir --break-system-packages apache-flink==2.3.0

USER flink


# ---------------------------------------------------------
# Flink Kafka Connector
# ---------------------------------------------------------
COPY lib/flink-connector-kafka-4.0.0-2.0.jar /opt/flink/lib/
COPY lib/kafka-clients-3.9.0.jar /opt/flink/lib/

# ---------------------------------------------------------
# Flink Parquet
# ---------------------------------------------------------
COPY lib/flink-parquet-2.3.0.jar /opt/flink/lib/

# ---------------------------------------------------------
# Hadoop dependencies
# ---------------------------------------------------------
COPY lib/hadoop/hadoop-common-2.10.2.jar /opt/flink/lib/
COPY lib/woodstox-core-6.6.2.jar /opt/flink/lib/
COPY lib/stax2-api-4.2.2.jar /opt/flink/lib/

COPY lib/hadoop/hadoop-hdfs-2.10.2.jar /opt/flink/lib/
COPY lib/hadoop/hadoop-mapreduce-client-core-2.10.2.jar /opt/flink/lib/

COPY lib/guava-27.0-jre.jar /opt/flink/lib/
COPY lib/commons-logging-1.2.jar /opt/flink/lib/

COPY lib/parquet/parquet-hadoop-1.15.2.jar /opt/flink/lib/
COPY lib/parquet/parquet-column-1.15.2.jar /opt/flink/lib/
COPY lib/parquet/parquet-common-1.15.2.jar /opt/flink/lib/
COPY lib/parquet/parquet-encoding-1.15.2.jar /opt/flink/lib/
COPY lib/parquet/parquet-format-structures-1.15.2.jar /opt/flink/lib/
COPY lib/parquet/parquet-jackson-1.15.2.jar /opt/flink/lib/

COPY KafkaCDCToMinIO.py /opt/flink/KafkaCDCToMinIO.py

# ---------------------------------------------------------
# Native S3 filesystem
# ---------------------------------------------------------
RUN mkdir -p /opt/flink/plugins/s3-fs-native \
    && cp /opt/flink/opt/flink-s3-fs-native-2.3.0.jar \
       /opt/flink/plugins/s3-fs-native/
