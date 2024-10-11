# Dockerfile.base
FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install basic dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

# Set the working directory
WORKDIR /app

# Copy the commons directory to the container
COPY ./src/commons /app/src/commons
COPY ./config /app/config
COPY ./src/requirements.txt /app/requirements.txt

# Install the commons package using pip
RUN pip install -r requirements.txt
RUN sed -i 's|from numpy import NaN as npNaN|from numpy import nan as npNaN|g' $(python3 -c 'import site; print(site.getsitepackages()[0])')/pandas_ta/momentum/squeeze_pro.py
# Command to run when container starts
CMD ["python"]
