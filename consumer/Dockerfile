# Updated Consumer Dockerfile
FROM maven:3.8.6-openjdk-11-slim

# Set working directory
WORKDIR /app

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

