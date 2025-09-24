FROM --platform=linux/arm64 apache/superset:latest

USER root
RUN pip install --no-cache-dir "cryptography<40" && \
    pip install --no-cache-dir snowflake-connector-python
USER superset