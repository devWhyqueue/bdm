# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# # Run producer.py when the container launches -> this should be started + orchestrated by airflow
# CMD ["python", "stock_stream_producer/producer.py"]
