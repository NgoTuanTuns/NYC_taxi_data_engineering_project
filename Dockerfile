FROM apache/airflow:3.3.1
COPY requirements.txt /
COPY data /data
RUN pip install -r requirements.txt

