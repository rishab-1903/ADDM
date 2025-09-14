#!/bin/bash

echo " WARNING: This will delete ALL containers, images, and volumes!"

echo " Stopping all running containers..."
docker stop $(docker ps -aq) 2>/dev/null

echo " Removing all containers..."
docker rm -f $(docker ps -aq) 2>/dev/null

echo " Removing all images..."
docker rmi -f $(docker images -aq) 2>/dev/null

echo " Removing all volumes..."
docker volume rm -f $(docker volume ls -q) 2>/dev/null

echo " Running docker system prune..."
docker system prune -af --volumes

echo " Docker cleanup complete."