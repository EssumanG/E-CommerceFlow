FROM apache/airflow:2.4.2
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==2.1.3