"""
Asset / Dataset definitions for Data-Aware Scheduling in Apache Airflow.
Compatible with both Airflow 3 (airflow.sdk.Asset) and Airflow 2.10+ (airflow.datasets.Dataset).
"""

try:
    from airflow.sdk import Asset
except ImportError:
    from airflow.datasets import Dataset as Asset

ADLS_RAW_ASSET = Asset("nyc_taxi/adls/raw")
BRONZE_ASSET = Asset("nyc_taxi/databricks/bronze")
