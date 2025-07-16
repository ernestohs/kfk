package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.errors.WakeupException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Kafka Consumer example with configurable message consumption
 * 
 * This consumer reads JSON messages from a Kafka topic and processes them.
 * Configuration can be customized through environment variables.
 */
public class KafkaConsumer {
    
    private static final Logger logger = Logger.getLogger(KafkaConsumer.class.getName());
    
    // Configuration from environment variables with defaults
    private static final String BOOTSTRAP_SERVERS = 
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String TOPIC_NAME = 
        System.getenv().getOrDefault("KAFKA_TOPIC", "user-events");
    private static final String GROUP_ID = 
        System.getenv().getOrDefault("KAFKA_GROUP_ID", "user-events-group");
    private static final long POLL_TIMEOUT = 
        Long.parseLong(System.getenv().getOrDefault("POLL_TIMEOUT", "1000"));
    
    private final Consumer<String, String> consumer;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running;
    
    public KafkaConsumer() {
        this.consumer = createConsumer();
        this.objectMapper = new ObjectMapper();
        this.running = new AtomicBoolean(true);
        
        // Add shutdown hook for graceful cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }
    
    /**
     * Creates and configures the Kafka consumer with optimal settings
     */
    private Consumer<String, String> createConsumer() {
        Properties props = new Properties();
        
        // Basic configuration
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // Consumer behavior settings
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Read from beginning
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);     // Manual commit for better control
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000); // Auto-commit interval (if enabled)
        
        // Performance settings
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);         // Min bytes to fetch
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);        // Max wait time for fetch
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);         // Max records per poll
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);     // Session timeout
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);  // Heartbeat interval
        
        // For exactly-once processing
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        
        logger.info("Consumer configured with bootstrap servers: " + BOOTSTRAP_SERVERS);
        logger.info("Consumer group ID: " + GROUP_ID);
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }
    
    /**
     * Processes a single message and extracts relevant information
     */
    private void processMessage(ConsumerRecord<String, String> record) {
        try {
            logger.info(String.format(
                "Processing message - Topic: %s, Partition: %d, Offset: %d, Key: %s",
                record.topic(), record.partition(), record.offset(), record.key()
            ));
            
            // Parse the JSON message
            JsonNode messageJson = objectMapper.readTree(record.value());
            
            // Extract and log key information
            String messageId = messageJson.has("messageId") ? messageJson.get("messageId").asText() : "unknown";
            String userId = messageJson.has("userId") ? messageJson.get("userId").asText() : "unknown";
            String eventType = messageJson.has("eventType") ? messageJson.get("eventType").asText() : "unknown";
            String timestamp = messageJson.has("timestamp") ? messageJson.get("timestamp").asText() : "unknown";
            
            logger.info(String.format(
                "Message Content - ID: %s, User: %s, Event: %s, Time: %s",
                messageId, userId, eventType, timestamp
            ));
            
            // Process event data if present
            if (messageJson.has("data")) {
                JsonNode data = messageJson.get("data");
                processEventData(data, eventType);
            }
            
            // Simulate message processing time
            Thread.sleep(100);
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to process message: " + record.value(), e);
            // In a real application, you might want to send this to a dead letter queue
        }
    }
    
    /**
     * Processes specific event data based on event type
     */
    private void processEventData(JsonNode data, String eventType) {
        try {
            switch (eventType) {
                case "USER_ACTION":
                    if (data.has("action")) {
                        String action = data.get("action").asText();
                        logger.info("User performed action: " + action);
                        
                        if ("purchase".equals(action) && data.has("amount")) {
                            double amount = data.get("amount").asDouble();
                            logger.info("Purchase amount: $" + String.format("%.2f", amount));
                        }
                    }
                    break;
                    
                case "SYSTEM_EVENT":
                    logger.info("Processing system event...");
                    break;
                    
                case "TRANSACTION":
                    logger.info("Processing transaction event...");
                    break;
                    
                case "ERROR":
                    logger.warning("Error event received - may need special handling");
                    break;
                    
                default:
                    logger.info("Processing unknown event type: " + eventType);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to process event data", e);
        }
    }
    
    /**
     * Starts the consumer loop that continuously polls for messages
     */
    public void startConsuming() {
        try {
            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            logger.info("Consumer started and subscribed to topic: " + TOPIC_NAME);
            
            long messageCount = 0;
            
            while (running.get()) {
                try {
                    // Poll for new messages
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
                    
                    if (records.isEmpty()) {
                        logger.fine("No new messages, continuing to poll...");
                        continue;
                    }
                    
                    logger.info("Received " + records.count() + " messages");
                    
                    // Process each message
                    for (ConsumerRecord<String, String> record : records) {
                        processMessage(record);
                        messageCount++;
                    }
                    
                    // Manually commit offsets after processing all messages in the batch
                    consumer.commitSync();
                    logger.info("Committed offsets for " + records.count() + " messages");
                    
                    // Log progress every 50 messages
                    if (messageCount % 50 == 0) {
                        logger.info("Total messages processed: " + messageCount);
                    }
                    
                } catch (WakeupException e) {
                    // Expected exception when shutting down
                    logger.info("Consumer wakeup received");
                    break;
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error during message consumption", e);
                    // Continue processing - don't stop the consumer for individual errors
                }
            }
            
            logger.info("Consumer loop ended. Total messages processed: " + messageCount);
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Fatal error in consumer", e);
        } finally {
            cleanup();
        }
    }
    
    /**
     * Gracefully shuts down the consumer
     */
    public void shutdown() {
        logger.info("Shutting down consumer...");
        running.set(false);
        consumer.wakeup(); // Interrupt the polling
    }
    
    /**
     * Cleanup resources
     */
    private void cleanup() {
        logger.info("Closing consumer...");
        if (consumer != null) {
            try {
                consumer.close();
                logger.info("Consumer closed successfully");
            } catch (Exception e) {
                logger.log(Level.WARNING, "Error closing consumer", e);
            }
        }
    }
    
    /**
     * Main method to run the consumer
     */
    public static void main(String[] args) {
        KafkaConsumer kafkaConsumer = new KafkaConsumer();
        
        try {
            kafkaConsumer.startConsuming();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Consumer failed", e);
        }
    }
}

