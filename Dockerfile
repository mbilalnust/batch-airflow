FROM apache/airflow:2.10.5

USER root
# Create directory for intermediate data
RUN mkdir -p /opt/airflow/data/intermediate && \
    chown -R airflow:root /opt/airflow/data

USER airflow
# Install Python packages
RUN pip install --no-cache-dir pandas numpy 