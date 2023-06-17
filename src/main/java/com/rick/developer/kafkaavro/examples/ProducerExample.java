package com.rick.developer.kafkaavro.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

    public class ProducerExample {
        public static void main(String[] args) {

            // Set up the configuration.
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("key.serializer", StringSerializer.class);
            props.put("value.serializer", StringSerializer.class);
            props.put("schema.registry.url", "http://localhost:8081");

            // Create the producer.
            Producer<String, String> producerImpl = new KafkaProducer<>(props);


            // Send messages to 'input-topic'
            for (int i = 0; i < 10; i++) {
                String value = "orderNumber-"+ (i*300);
                String key = i + "";
                ProducerRecord<String, String> record = new ProducerRecord<>("input-stream-topic", key ,value);
                producerImpl.send(record);
            }

            // Close the producer.
            producerImpl.close();
        }
    }

