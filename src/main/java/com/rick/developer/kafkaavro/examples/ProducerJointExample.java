package com.rick.developer.kafkaavro.examples;

import com.rick.developer.kafkaavro.ApplianceOrder;
import com.rick.developer.kafkaavro.ElectronicOrder;
import com.rick.developer.kafkaavro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

public class ProducerJointExample {
    public static void main(String[] args) {

        // Set up the configuration.
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        // Create the producer.
        Producer<String, ApplianceOrder> producerImpl = new KafkaProducer<>(props);

        for (int i = 0; i < 2; i++) {
            ApplianceOrder order = new ApplianceOrder("remodel-"+i, "dishwasher-1333", "10261998", Instant.now().toEpochMilli());
            String key = i + "";
            ProducerRecord<String, ApplianceOrder> record = new ProducerRecord<>("streams-left-side-input", key ,order);
            producerImpl.send(record);
        }

        Producer<String, ElectronicOrder> producerImplElec = new KafkaProducer<>(props);
        for (int i = 0; i < 2; i++) {
            ElectronicOrder order = new ElectronicOrder("remodel-"+i, "laptop-5333", "10261999", 0.0, Instant.now().toEpochMilli());
            String key = i + "";
            ProducerRecord<String, ElectronicOrder> record = new ProducerRecord<>("streams-right-side-input", key ,order);
            producerImplElec.send(record);
        }

        Producer<String, User> producerImplUser = new KafkaProducer<>(props);
        for (int i = 0; i < 2; i++) {
            User user = new User("hello"+i, i);
            String key = i + "";
            ProducerRecord<String, User> record = new ProducerRecord<>("streams-join-table-input", key ,user);
            producerImplUser.send(record);
        }

        Producer<String, ElectronicOrder> producerImplAggr = new KafkaProducer<>(props);
        // Send messages to 'aggregate-input-topic'
        for (int i = 0; i < 3; i++) {
            ElectronicOrder order = new ElectronicOrder("remodel-"+i, "laptop-5333", "10261999", 0.0 + i*4, Instant.now().toEpochMilli());
            String key = i + "";
            ProducerRecord<String, ElectronicOrder> record = new ProducerRecord<>("aggregate-input-topic", key ,order);
            producerImplElec.send(record);
        }


        // Close the producer.
        producerImpl.close();
        producerImplElec.close();
        producerImplUser.close();
        producerImplAggr.close();
    }
}

