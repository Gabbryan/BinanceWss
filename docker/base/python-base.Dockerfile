# Use an official Python runtime as a parent image
FROM python:3.10-slim as base

# Set the working directory in the container
WORKDIR /app

# Install any necessary dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and install libs Python dependencies
RUN python -m pip install --upgrade pip

# Use a second stage to keep the final image small
FROM python:3.10-slim

# Copy dependencies from the base stage
COPY --from=base /usr/local /usr/local



# Set the working directory in the container
WORKDIR /app


# Default command to run when the container starts
CMD ["python3"]
