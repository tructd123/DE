# Use a specific Python version for reproducibility
FROM python:3.10-slim

# Set the working directory
WORKDIR /opt/dagster/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the gRPC port
EXPOSE 4000
