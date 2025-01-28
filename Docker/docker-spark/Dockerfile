FROM bitnami/spark:latest



USER root
RUN pip install psycopg2-binary

RUN apt-get update && apt-get install -y wget &&\
    wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar &&\
    mv postgresql-42.5.0.jar /opt/bitnami/spark/jars/ &&\
    apt-get clean && rm -rf /var/lib/apt/lists/*
    