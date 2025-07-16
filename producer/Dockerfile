# Simple Dockerfile for Kafka Producer
FROM maven:3.9.4-openjdk-11-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY pom.xml .
COPY src/ ./src/

# Build the application
RUN mvn clean package -DskipTests -q

# Create non-root user
RUN groupadd -r kafka && useradd -r -g kafka kafka
RUN chown -R kafka:kafka /app
USER kafka

# Environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC=user-events
ENV PRODUCER_INTERVAL=5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD pgrep -f "kafka-producer" || exit 1

# Run the application
CMD ["java", "-jar", "target/kafka-producer.jar"]

