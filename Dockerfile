# Dockerfile for building image with all ExDB CLI commands 
# and packages needed for running ETL workflow

# Use an official Python image as a base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements file
COPY ./requirements.txt /app/requirements.txt

# Install system dependencies
RUN apt-get update \
    # Confirmed versions that work: build-essential=12.9 pkg-config=1.8.1-1 default-libmysqlclient-dev=1.1.0
    && apt-get install -y build-essential=12.* pkg-config=1.8.* default-libmysqlclient-dev=1.1.*

# Install the required Python packages
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir --user -r /app/requirements.txt \
    && pip install --no-cache-dir pymongo==3.12.0

# Specify the command to run on container start
# CMD ["python", "script.py"]
