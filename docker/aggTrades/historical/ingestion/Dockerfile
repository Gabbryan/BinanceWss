# Use the locally built resilient base image
FROM python:3.10-slim as base

# Prevents Python from writing pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1
# Keeps Python from buffering stdout and stderr (facilitates logging)
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY docker/aggTrades/historical/ingestion/requirements.txt .

# Copy the application code into the container
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and install libs Python dependencies
RUN python -m pip install --upgrade pip
# Install dependencies using pip
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Copy the list_contents.sh script into the container and make it executable


# Run the script to list the contents of the /app directory

# Set the timezone to Paris
RUN ln -fs /usr/share/zoneinfo/Europe/Paris /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

# Command to run the application. Adjust the script name as per your application's entry point.
