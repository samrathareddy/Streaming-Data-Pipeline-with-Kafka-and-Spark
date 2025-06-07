#!/bin/bash

# Ensure Docker and Docker Compose are installed
if ! [ -x "$(command -v docker)" ] || ! [ -x "$(command -v docker-compose)" ]; then
  echo "Docker and Docker Compose are required. Please install them first."
  exit 1
fi

# Pull necessary images
echo "Pulling Docker images..."
docker pull confluentinc/cp-zookeeper:7.3.2
docker pull confluentinc/cp-kafka:7.3.2
docker pull mysql:8.0
docker pull postgres:14
docker pull minio/minio:latest

# Build and start the containers using Docker Compose
echo "Starting the data pipeline..."
docker-compose up -d --build

# Check the status of the containers
echo "Checking container status..."
docker ps

echo "Data pipeline setup completed successfully."
