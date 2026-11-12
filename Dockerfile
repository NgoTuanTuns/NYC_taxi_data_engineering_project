FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-databricks \
    apache-airflow-providers-microsoft-azure \
    apache-airflow-providers-databricks
