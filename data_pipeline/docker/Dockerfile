FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    python3 \
    python3-pip \
    curl \
    tar \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

ARG TARGETARCH
ENV SPARK_VERSION=4.1.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin

RUN curl -fL "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" | tar xz -C /opt && \
    mv /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN uv pip install --system --no-cache-dir \
    pyspark==$SPARK_VERSION \
    pandas \
    pyarrow \
    jupyterlab \
    matplotlib \
    seaborn \
    meteostat

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN useradd -m -s /bin/bash spark
RUN mkdir -p /opt/spark/jobs /opt/spark/data /opt/spark/spark-events /opt/spark/logs /opt/spark/notebooks
COPY conf/* /opt/spark/conf/
RUN chown -R spark:spark $SPARK_HOME

COPY entry.sh /entry.sh
RUN chmod +x /entry.sh && chown spark:spark /entry.sh

WORKDIR $SPARK_HOME
USER spark

ENTRYPOINT ["/entry.sh"]
