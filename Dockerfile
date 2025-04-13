# Dockerfile

# Use an official Python runtime as a parent image
# Choose a version compatible with your desired PySpark version (3.9 is usually safe)
FROM python:3.9-slim-buster

# Set Environment Variables
# Correct ENV lines from the example:
# Dockerfile (Using Bullseye Base Image)

# Use a newer base image based on Debian Bullseye
FROM python:3.9-slim-bullseye

# --- Keep ALL ENV lines as they were ---
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
# ... etc ...

# --- Use the ORIGINAL combined install command ---
# Package names are generally the same between Buster and Bullseye for these tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    wget \
    tar \
    bash \
    curl \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --- Keep the rest of the Dockerfile (Spark download, WORKDIR, COPY, etc.) the same ---
# ...

# Download and install Spark
# Note: Adjust download URL if needed for different Spark/Hadoop versions
# Go to https://spark.apache.org/downloads.html to find URLs
RUN wget -qO- https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm -rf ${SPARK_HOME}/examples ${SPARK_HOME}/data

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# Using --no-cache-dir reduces image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container at /app
# This includes your src/ directory
COPY . .

# (Optional) Create a non-root user to run the application
# RUN useradd -ms /bin/bash sparky
# USER sparky
# If using a non-root user, you might need to adjust permissions on /opt/spark or /app/data if issues arise

# Default command (optional, can be overridden)
# CMD ["python", "src/processing.py"]