package com.simsekonline.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
	static Logger logger = LoggerFactory.getLogger(Producer.class);

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		for (int i = 1; i < 10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("second_topic",
					"Hello World :" + i);

			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						logger.info("\nMessage Sent:\n Offset :" + metadata.offset() + "\n Partition :"
								+ metadata.partition());

					} else {
						logger.info("Message Sent:" + exception);
					}
				}
			});			
			producer.flush();
		}
		producer.close();
	}

}
