package com.rick.developer.kafkaavro.examples;

import com.rick.developer.kafkaavro.ApplianceOrder;
import com.rick.developer.kafkaavro.CombinedOrder;
import com.rick.developer.kafkaavro.User;
import com.rick.developer.kafkaavro.utils.AvroSerdeUtils;
import com.rick.developer.kafkaavro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class AggregateExample {

    public static void main(String[] args) {

        Properties streamsProps = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream("src/main/resources/streams.properties")){
            streamsProps.load(fileInputStream);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-example");

        String inputTopic = streamsProps.getProperty("aggregate.input.topic");
        String outputTopic = streamsProps.getProperty("aggregate.output.topic");

        StreamsBuilder builder = new StreamsBuilder();
        SpecificAvroSerde<ElectronicOrder> orderSerde = AvroSerdeUtils.getSpecificAvroSerde();

        KStream<String, ElectronicOrder> orderKStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), orderSerde))
                .peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));

        orderKStream.groupByKey().reduce(((value1, value2) -> value2), Materialized.with(Serdes.String(), orderSerde))
                        .toStream().peek((key, value) -> System.out.println("Outgoing record of reduce, key: "+ key + "value: "+ value));

        orderKStream.groupByKey().aggregate(()-> 0.0, (key, order, total) ->total + order.getPrice(),
                Materialized.with(Serdes.String(), Serdes.Double())).toStream()
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();
    }

    public static class JoinExample {
        public static void main(String[] args) {
            Properties streamsProps = new Properties();
            try (FileInputStream fileInputStream = new FileInputStream("src/main/resources/streams.properties")){
                streamsProps.load(fileInputStream);
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
            streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "joint-streams");

            final String streamOneInput = streamsProps.getProperty("stream_one.input.topic");
            final String streamTwoInput = streamsProps.getProperty("stream_two.input.topic");
            final String tableInput = streamsProps.getProperty("table.input.topic");
            final String outputTopic = streamsProps.getProperty("joins.output.topic");

            SpecificAvroSerde<ApplianceOrder> applianceSerde = AvroSerdeUtils.getSpecificAvroSerde();
            SpecificAvroSerde<ElectronicOrder> electronicSerde = AvroSerdeUtils.getSpecificAvroSerde();
            SpecificAvroSerde<User> userSerde = AvroSerdeUtils.getSpecificAvroSerde();
            SpecificAvroSerde<CombinedOrder> combinedSerde = AvroSerdeUtils.getSpecificAvroSerde();

            ValueJoiner<ApplianceOrder, ElectronicOrder, CombinedOrder> orderJoiner =
                    (applianceOrder, electronicOrder) -> CombinedOrder.newBuilder()
                            .setApplianceOrderId(applianceOrder.getOrderId())
                            .setApplianceId(applianceOrder.getApplianceId())
                            .setElectronicOrderId(electronicOrder.getOrderId())
                            .setTime(electronicOrder.getTime())
                            .build();

            ValueJoiner<CombinedOrder, User, CombinedOrder> enrichmentJoiner =
                    (combinedOrder, user) -> {
                        if (user != null) {
                            combinedOrder.setUserName(user.getName());
                        }

                        return combinedOrder;
                    };

            StreamsBuilder builder = new StreamsBuilder();

            KStream<String, ApplianceOrder> applianceStream =
                    builder.stream(streamOneInput, Consumed.with(Serdes.String(), applianceSerde))
                            .peek((key, value) -> System.out.println("Appliance stream incoming record key " + key + " value " + value));

            KStream<String, ElectronicOrder> electronicStream =
                    builder.stream(streamTwoInput, Consumed.with(Serdes.String(), electronicSerde))
                            .peek((key, value) -> System.out.println("Electronic stream incoming record " + key + " value " + value));

            KStream<String, CombinedOrder> combinedStream = applianceStream.join(
                    electronicStream, orderJoiner,
                    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(30)),
                    StreamJoined.with(Serdes.String(), applianceSerde, electronicSerde))
                    .peek((key, value) -> System.out.println("Stream-Stream Join record key " + key + " value " + value));;

            KTable<String, User> userTable = builder.table(tableInput, Materialized.with(Serdes.String(), userSerde));
            combinedStream.leftJoin(
                            userTable,
                            enrichmentJoiner,
                            Joined.with(Serdes.String(), combinedSerde, userSerde))
                    .peek((key, value) -> System.out.println("Stream-Table Join record key " + key + " value " + value))
                    .to(outputTopic, Produced.with(Serdes.String(), combinedSerde));


            KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
            kafkaStreams.start();
        }
    }
}
