# Use the locally built resilient base image
FROM python:3.10-slim AS base
# Prevents Python from writing pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1
# Keeps Python from buffering stdout and stderr (facilitates logging)
ENV PYTHONUNBUFFERED=1


# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY /libs/ingestion-data-vision-binance/src/requirements.txt .

# Install dependencies using pip
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt


# Set the working directory in the container
WORKDIR /app
