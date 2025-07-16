#!/bin/bash

# Kafka Producer Docker Build Script
set -e

# Configuration
IMAGE_NAME="kafka-producer"
IMAGE_TAG="latest"
CONTAINER_NAME="kafka-producer-container"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    log_error "Docker is not running. Please start Docker first."
    exit 1
fi

# Parse command line arguments
BUILD_ONLY=false
RUN_CONTAINER=false
CLEAN_BUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--build-only)
            BUILD_ONLY=true
            shift
            ;;
        -r|--run)
            RUN_CONTAINER=true
            shift
            ;;
        -c|--clean)
            CLEAN_BUILD=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  -b, --build-only    Build the image only"
            echo "  -r, --run           Build and run the container"
            echo "  -c, --clean         Clean build (no cache)"
            echo "  -h, --help          Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Default behavior if no flags provided
if [ "$BUILD_ONLY" = false ] && [ "$RUN_CONTAINER" = false ]; then
    BUILD_ONLY=true
fi

# Clean build if requested
if [ "$CLEAN_BUILD" = true ]; then
    log_info "Performing clean build..."
    BUILD_ARGS="--no-cache"
else
    BUILD_ARGS=""
fi

# Build the Docker image
log_info "Building Docker image: $IMAGE_NAME:$IMAGE_TAG"
if docker build $BUILD_ARGS -t $IMAGE_NAME:$IMAGE_TAG .; then
    log_info "Docker image built successfully"
else
    log_error "Failed to build Docker image"
    exit 1
fi

# Run container if requested
if [ "$RUN_CONTAINER" = true ]; then
    log_info "Stopping existing container if running..."
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
    
    log_info "Starting new container..."
    docker run -d \
        --name $CONTAINER_NAME \
        --network kafka-example_kafka-network \
        -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
        -e KAFKA_TOPIC=user-events \
        -e PRODUCER_INTERVAL=5000 \
        $IMAGE_NAME:$IMAGE_TAG
    
    log_info "Container started successfully"
    log_info "View logs with: docker logs -f $CONTAINER_NAME"
fi

# Show image info
log_info "Docker image information:"
docker images | grep $IMAGE_NAME

# Show running containers
if [ "$RUN_CONTAINER" = true ]; then
    log_info "Running containers:"
    docker ps | grep -E "(CONTAINER|$CONTAINER_NAME)"
fi

log_info "Build process completed successfully!"

