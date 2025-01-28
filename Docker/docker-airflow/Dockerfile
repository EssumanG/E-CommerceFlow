FROM apache/airflow:2.4.2
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==2.1.3 python-dotenv kagglehub
USER root
RUN apt-get update &&\
    apt-get install -y openjdk-11-jdk