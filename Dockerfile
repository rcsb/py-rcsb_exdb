# Dockerfile for building image with all ExDB CLI commands 
# and packages needed for running ETL workflow

# Use an official Python image as a base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# copy requirements file (should include selected versions of uvicorn gunicorn)
COPY ./requirements.txt /app/requirements.txt

# Install system dependencies
RUN apt-get update \
    && apt-get install -y build-essential pkg-config default-libmysqlclient-dev

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir --user -r /app/requirements.txt \
    && pip install --no-cache-dir rcsb.workflow rcsb.utils.io pymongo

# Install the required Python packages
# RUN pip install --no-cache-dir rcsb.db rcsb.exdb

# Specify the command to run on container start
# CMD ["python", "script.py"]
