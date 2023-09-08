# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster
LABEL maintainer="Cristian"

# Arguments
ARG AIRFLOW_VERSION=2.7.0  # Update this to the latest version
ARG AIRFLOW_HOME=/opt/airflow

# Environment Variables
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Install system dependencies
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python packages
RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    pip install "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.8.txt" && \ 
      pip install --no-cache-dir -r /requirements.txt  

# Set permissions
RUN chown -R airflow: ${AIRFLOW_HOME}

# Copy and set permissions for the start script
COPY ./start-airflow.sh ./start-airflow.sh
RUN chmod +x ./start-airflow.sh

# Switch to airflow user
USER airflow

# Create dags folder
RUN mkdir -p ${AIRFLOW_HOME}/dags

# Expose port
EXPOSE 8080

# Set entrypoint
CMD [ "./start-airflow.sh" ]
