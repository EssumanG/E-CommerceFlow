FROM jupyter/pyspark-notebook:latest

USER root

# Set working directory inside the container
WORKDIR /home/jovyan/work

# Copy requirements file (Ensure it's in the same directory as Dockerfile)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Change back to `jovyan` user for security
USER jovyan