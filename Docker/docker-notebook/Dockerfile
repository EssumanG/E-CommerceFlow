FROM jupyter/pyspark-notebook:latest

USER root

RUN apt-get update && apt-get install -y wget &&\
    python3 -m pip install --upgrade pip &&\
    pip install psycopg2-binary &&\
    apt-get clean && rm -rf /var/lib/apt/lists/*