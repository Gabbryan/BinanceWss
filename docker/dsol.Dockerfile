# Dockerfile.dsol
FROM base AS dsol

# Copy the DSOL-specific setup file
COPY ./src/libs/ /app/src/libs/


# Install DSOL-specific dependencies
RUN pip install /app/src/libs


