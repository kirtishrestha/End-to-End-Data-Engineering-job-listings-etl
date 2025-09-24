FROM bitnami/spark:3.5 AS spark_base

FROM apache/airflow:2.8.1-python3.11

COPY --from=spark_base /opt/bitnami/spark /opt/bitnami/spark

ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER root

RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jdk \
        build-essential \
        curl \
        netcat-openbsd \
        libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt