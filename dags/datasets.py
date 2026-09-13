from airflow.sdk import Asset

ADLS_RAW_ASSET    = Asset("nyc_taxi/adls/raw")
BRONZE_ASSET      = Asset("nyc_taxi/databricks/bronze")
