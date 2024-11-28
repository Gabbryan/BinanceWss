FROM bitnami/spark:latest

# Set the working directory
WORKDIR /opt/bitnami/spark/app

# Copy the requirements.txt file and .env into the image
COPY ./cores/aggTrades/historical/transformation/src/requirements.txt .
COPY ./.env .

# Switch to root user to install dependencies and set permissions
USER root

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Ensure that the Spark work directory is writable
RUN mkdir -p /opt/bitnami/spark/work && chmod -R 777 /opt/bitnami/spark/work

# Switch back to the default user (bitnami)

# Set the working directory back to the app directory
WORKDIR /opt/bitnami/spark/app
