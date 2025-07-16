package com.example.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Kafka Producer example with configurable message publishing
 * 
 * This producer sends JSON messages to a Kafka topic at regular intervals.
 * Messages can be customized through environment variables and configuration.
 */
public class KafkaProducer {
    
    private static final Logger logger = Logger.getLogger(KafkaProducer.class.getName());
    
    // Configuration from environment variables with defaults
    private static final String BOOTSTRAP_SERVERS = 
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String TOPIC_NAME = 
        System.getenv().getOrDefault("KAFKA_TOPIC", "user-events");
    private static final long PRODUCER_INTERVAL = 
        Long.parseLong(System.getenv().getOrDefault("PRODUCER_INTERVAL", "5000"));
    
    private final Producer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final Random random;
    
    public KafkaProducer() {
        this.producer = createProducer();
        this.objectMapper = new ObjectMapper();
        this.random = new Random();
        
        // Add shutdown hook for graceful cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(this::cleanup));
    }
    
    /**
     * Creates and configures the Kafka producer with optimal settings
     */
    private Producer<String, String> createProducer() {
        Properties props = new Properties();
        
        // Basic configuration
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Performance and reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all");  // Wait for all replicas
        props.put(ProducerConfig.RETRIES_CONFIG, 3);   // Retry failed sends
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // Batch size in bytes
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);     // Wait up to 10ms for batching
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB buffer
        
        // Idempotence for exactly-once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Compression for better throughput
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        logger.info("Producer configured with bootstrap servers: " + BOOTSTRAP_SERVERS);
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }
    
    /**
     * Creates a sample JSON message with user event data
     */
    private String createMessage(int messageId) {
        try {
            ObjectNode message = objectMapper.createObjectNode();
            message.put("messageId", messageId);
            message.put("userId", "user_" + random.nextInt(1000));
            message.put("eventType", getRandomEventType());
            message.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            message.put("data", createEventData());
            
            return objectMapper.writeValueAsString(message);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to create message", e);
            return "{}";
        }
    }
    
    /**
     * Creates sample event data based on event type
     */
    private ObjectNode createEventData() {
        ObjectNode data = objectMapper.createObjectNode();
        String[] actions = {"login", "logout", "purchase", "view_product", "add_to_cart"};
        String action = actions[random.nextInt(actions.length)];
        
        data.put("action", action);
        data.put("sessionId", "session_" + random.nextInt(10000));
        
        if ("purchase".equals(action)) {
            data.put("amount", random.nextDouble() * 1000);
            data.put("currency", "USD");
        }
        
        return data;
    }
    
    /**
     * Returns a random event type for demonstration
     */
    private String getRandomEventType() {
        String[] eventTypes = {"USER_ACTION", "SYSTEM_EVENT", "TRANSACTION", "ERROR"};
        return eventTypes[random.nextInt(eventTypes.length)];
    }
    
    /**
     * Sends a single message to Kafka asynchronously
     */
    public void sendMessage(String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, message);
        
        // Send asynchronously with callback
        Future<RecordMetadata> future = producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    logger.log(Level.SEVERE, "Failed to send message with key: " + key, exception);
                } else {
                    logger.info(String.format(
                        "Message sent successfully - Topic: %s, Partition: %d, Offset: %d, Key: %s",
                        metadata.topic(), metadata.partition(), metadata.offset(), key
                    ));
                }
            }
        });
        
        // Optional: Get metadata synchronously (blocks)
        // try {
        //     RecordMetadata metadata = future.get();
        //     logger.info("Message sent to partition " + metadata.partition() + " with offset " + metadata.offset());
        // } catch (Exception e) {
        //     logger.log(Level.SEVERE, "Failed to send message", e);
        // }
    }
    
    /**
     * Starts the producer loop that sends messages at regular intervals
     */
    public void startProducing() {
        logger.info("Starting Kafka producer...");
        logger.info("Sending messages to topic: " + TOPIC_NAME);
        logger.info("Producer interval: " + PRODUCER_INTERVAL + "ms");
        
        int messageCounter = 0;
        
        try {
            while (!Thread.currentThread().isInterrupted()) {
                messageCounter++;
                String key = "key_" + messageCounter;
                String message = createMessage(messageCounter);
                
                sendMessage(key, message);
                
                // Log every 10th message for visibility
                if (messageCounter % 10 == 0) {
                    logger.info("Sent " + messageCounter + " messages so far...");
                }
                
                Thread.sleep(PRODUCER_INTERVAL);
            }
        } catch (InterruptedException e) {
            logger.info("Producer interrupted, shutting down...");
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Cleanup resources
     */
    public void cleanup() {
        logger.info("Closing producer...");
        if (producer != null) {
            producer.flush(); // Ensure all messages are sent
            producer.close();
        }
        logger.info("Producer closed successfully");
    }
    
    /**
     * Main method to run the producer
     */
    public static void main(String[] args) {
        KafkaProducer kafkaProducer = new KafkaProducer();
        
        try {
            kafkaProducer.startProducing();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Producer failed", e);
        } finally {
            kafkaProducer.cleanup();
        }
    }
}

