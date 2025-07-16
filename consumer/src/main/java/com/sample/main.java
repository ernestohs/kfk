package com.sample;

import org.apache.kafka.*;
import java.time.Duration;

public class Consumer {
	public static void main(String[] args) {
		String topic = "mauricio";

		Properties props = new Properties();
		props.put(BOOTSTRAP_SERVER_CONFIG, "localhost:9092");
		Consumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(topic));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.OfMillis(1000));
			for (ConsumerRecord<String, String> record: records) {
				System.out.println("Consumed: key=%s value=%s offset=%d", record.key(), record.value(), record.offset());
			}
		}
	}
}
