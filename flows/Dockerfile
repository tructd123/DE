# Use a specific Python version for reproducibility
FROM python:3.10-slim

# Set the working directory
WORKDIR /opt/dagster/app

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the Dagster webserver port
EXPOSE 3000
