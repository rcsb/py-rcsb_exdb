# Dockerfile for building image with all ExDB CLI commands 
# and packages needed for running ETL workflow

# Use an official Python image as a base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app
ENV PATH=$PATH:/root/.local/bin
    OE_LICENSE=/opt/etl-scratch/config/oe_license.txt
    NLTK_DATA=/opt/etl-scratch/data/nltk_data
    CONFIG_SUPPORT_TOKEN_ENV=73ea2e9b2964758418f04d0a5dad069674467bbadb78f7c0558b57ed302d1e92

# Copy requirements file
COPY ./requirements.txt /app/requirements.txt

# Install system dependencies
RUN apt-get update \
    # Confirmed versions that work: build-essential=12.9 pkg-config=1.8.1-1 default-libmysqlclient-dev=1.1.0
    && apt-get install -y --no-install-recommends build-essential=12.* pkg-config=1.8.* default-libmysqlclient-dev=1.1.* \
    && rm -rf /var/lib/apt/lists/*

# Install the required Python packages
RUN pip install --no-cache-dir --upgrade "pip>=23.0.0" "setuptools>=40.8.0" "wheel>=0.43.0" \
    && pip install --no-cache-dir --user -r /app/requirements.txt \
    && pip install --no-cache-dir pymongo==3.12.0

# Install the latest version of THIS packages
RUN pip install --no-cache-dir "rcsb.exdb>=1.21"
