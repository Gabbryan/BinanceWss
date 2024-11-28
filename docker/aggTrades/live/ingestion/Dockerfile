# Use the locally built resilient base image
FROM python:3.10-slim as base
# Prevents Python from writing pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1
# Keeps Python from buffering stdout and stderr (facilitates logging)
ENV PYTHONUNBUFFERED=1
# Set Matplotlib configuration directory
ENV MPLCONFIGDIR=/home/appuser/.config/matplotlib

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY docker/libs/wss-services/requirements.txt .

# Install dependencies using pip
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# Set the timezone to Paris
RUN ln -fs /usr/share/zoneinfo/Europe/Paris /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

# Set the working directory in the container
WORKDIR /app
