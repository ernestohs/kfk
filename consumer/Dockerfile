# Consumer Dockerfile
FROM openjdk:11-jre-slim

# Set working directory
WORKDIR /app

# Install Maven (for building the application)
RUN apt-get update && \
    apt-get install -y maven && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the source code and pom.xml
COPY pom.xml .
COPY src/ ./src/

# Build the application
RUN mvn clean package -q -DskipTests

# Create a non-root user for security
RUN groupadd -r kafka && useradd -r -g kafka kafka
RUN chown -R kafka:kafka /app
USER kafka

# Environment variables with defaults
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV KAFKA_TOPIC=user-events
ENV KAFKA_GROUP_ID=user-events-group
ENV POLL_TIMEOUT=1000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD pgrep -f "KafkaConsumer" > /dev/null || exit 1

# Command to run the consumer
CMD ["java", "-jar", "target/kafka-consumer.jar"]

