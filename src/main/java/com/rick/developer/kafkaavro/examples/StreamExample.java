package com.rick.developer.kafkaavro.examples;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StreamExample {

    public static void main(String[] args) {
        // Set up the configuration.
        Properties streamsProps = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream("src/main/resources/streams.properties")){
            streamsProps.load(fileInputStream);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Define the processing topology.
        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("basic.input.topic");
        final String outputTopic = streamsProps.getProperty("basic.output.topic");

        final String orderNumberStart = "orderNumber-";
        KStream<String, String> firstStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        firstStream.peek((key, value) -> System.out.println("incoming record  -> key: " + key + "value: " + value))
                    .filter((key, value) -> value.contains(orderNumberStart))
                    .mapValues(value -> value.substring(value.indexOf("-") + 1))
                    .filter((key, value) -> Long.parseLong(value) > 1000)
                    .peek((key, value) -> System.out.println("outcome record  -> key: " + key + "value: " + value))
                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        // Check out the reduce operation
        firstStream.groupByKey().reduce((v1, v2) -> v1 + v2).
                toStream().peek((key, value) -> System.out.println("outgoing record of reduce, key: "+ key + "value: "+ value));

        // Build and start the stream.
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();

        // Add shutdown hook for graceful shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
