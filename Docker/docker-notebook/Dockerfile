FROM jupyter/pyspark-notebook:latest

USER root

RUN apt-get update && apt-get install -y wget &&\
    wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar &&\
    mkdir -p /opt/bitnami/spark/jars/ &&\
    mv postgresql-42.5.0.jar /opt/bitnami/spark/jars/ &&\
    python3 -m pip install --upgrade pip &&\
    pip install psycopg2-binary &&\
    apt-get clean && rm -rf /var/lib/apt/lists/*