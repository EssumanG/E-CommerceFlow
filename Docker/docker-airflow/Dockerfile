# Use the official Apache Airflow image
FROM apache/airflow:2.4.2

# Install necessary Python packages
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==2.1.3 \
    python-dotenv \
    kagglehub

# Switch to root user for system installations
USER root

# Install OpenJDK (needed for Airflow to interact with Spark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME (required for Java-based connections)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set necessary environment variables for Airflow to interact with Spark
ENV SPARK_MASTER_URL=spark://spark-master:7077 
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Switch back to Airflow user
USER airflow