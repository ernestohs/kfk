package com.sample;

import org.apache.kafka.*;

import java.util.properties;

public class KafkaProduder {
	public static void main(String[] args) {
		String topic = "mauricio"
		Properties props = new Properties();
		props.put(BOOTSTRAP_SERVER_CONFIG, "localhost:9092");

		Producer<String, String> producer = new KafkaProducer<>()

		for (int i = 0; i < 10; i++) {
			String message = "Message #" + i;
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key-"+i, message);
			producer.send(record);
		}

		producer.close();
	}
}
