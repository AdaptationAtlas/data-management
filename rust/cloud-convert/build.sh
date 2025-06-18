#!/bin/bash

# Build your GDAL tool in a container (this creates the image)
# docker build -t gdal-rust-builder .
#
if ! docker buildx build -t gdal-rust-builder .; then
  echo "Build failed, cleaning up dangling build cache and images..."
  docker builder prune -f
  docker image prune -f
  exit 1
fi

# Extract the binary (use the same name you just built)
docker create --name temp-container gdal-rust-builder

# Copy from the correct path (glibc build, not musl)
docker cp temp-container:/app/target/x86_64-unknown-linux-gnu/release/cloud-convert ./

# Clean up
docker rm temp-container
