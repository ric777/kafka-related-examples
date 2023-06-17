package com.rick.developer.kafkaavro.examples;

import com.rick.developer.kafkaavro.User;
import com.rick.developer.kafkaavro.utils.AvroSerdeUtils;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TableExample {

    public static void main(String[] args) {
        // Set up the configuration.
        Properties streamsProps = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream("src/main/resources/streams.properties")){
            streamsProps.load(fileInputStream);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        // Set up the specific serde
        SpecificAvroSerde<User> avroSerde = AvroSerdeUtils.getSpecificAvroSerde();

        StreamsBuilder builder = new StreamsBuilder();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-tables-user");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, avroSerde.getClass());

        final String inputTopic = streamsProps.getProperty("basic.input.table.topic");
        final String outputTopic = streamsProps.getProperty("basic.output.table.topic");
        final String orderNumberStart = "orderNumber-";

        KTable<String, User> table = builder.table(inputTopic, Materialized.as("example-user"));
        table.filter((key, value) -> value.getName().contains(orderNumberStart))
                .mapValues(value -> value.getName().substring(value.getName().indexOf("-") + 1))
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .toStream()
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();

        // Wait for the streams instance to be fully started
        while (!kafkaStreams.state().equals(KafkaStreams.State.RUNNING)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        ReadOnlyKeyValueStore<String, User> example = kafkaStreams.store(StoreQueryParameters.fromNameAndType("example-user", QueryableStoreTypes.keyValueStore()));
        System.out.println("======= get value from example ========");
        System.out.println(example.get("100"));
        System.out.println("======= get value from example ========");

    }

}
