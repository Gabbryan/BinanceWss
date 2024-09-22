# Dockerfile for spark-base
FROM apache/spark-py:latest

# Switch to root user to ensure permissions for creating directories
USER root

# Install SSH server and other dependencies
RUN apt-get update && apt-get install -y openssh-server

# Create necessary directories
RUN mkdir -p /opt/spark/spark-events /opt/spark/work-dir
COPY ./conf/* /opt/spark/conf/

# Create the SSH run directory
RUN mkdir /var/run/sshd

# Add a new user for SSH with passwordless sudo
RUN useradd -ms /bin/bash spark && echo 'spark:spark' | chpasswd && adduser spark sudo

# Set the root password to allow SSH login
RUN echo 'root:root' | chpasswd

# Allow root login via SSH
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config



# Expose necessary ports for Spark and SSH
EXPOSE 22 7077 8080 8081 7337

# Start SSH service and default command
CMD service ssh start && bash
