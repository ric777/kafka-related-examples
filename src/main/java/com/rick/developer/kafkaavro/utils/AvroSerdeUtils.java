package com.rick.developer.kafkaavro.utils;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

import java.util.Collections;

public class AvroSerdeUtils {

    // Define the static method to getSpecificAvroSerde
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde() {
        SpecificAvroSerde<T> avroSerde = new SpecificAvroSerde<>();
        final boolean isKeySerde = false;
        avroSerde.configure(
                Collections.singletonMap("schema.registry.url", "http://localhost:8081"),
                isKeySerde);
        return avroSerde;
    }

}
