# Use the official Python image from the Docker Hub
FROM python:3.10-slim

# Set environment variables to ensure non-interactive apt installs
ENV DEBIAN_FRONTEND=noninteractive

# Install build-essential and other dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the requirements file into the container
COPY requirements.txt ./

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY ../../../../cores/aggTrades/live/validation/src .
RUN ls

# Ensure the .env file is included
COPY ../../../../cores/aggTrades/live/validation/src/.env .env

#Use Paris timezone
RUN ln -fs /usr/share/zoneinfo/Europe/Paris /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

# Command to run the script
CMD ["python", "main.py"]
