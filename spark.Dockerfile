FROM bitnami/spark:3.5

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl python3.11 && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11 - --break-system-packages && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER 1001