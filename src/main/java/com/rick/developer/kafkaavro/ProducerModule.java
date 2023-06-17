package com.rick.developer.kafkaavro;

import lombok.extern.apachecommons.CommonsLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
@CommonsLog(topic = "Producer Logger")
public class ProducerModule {

    @Value("${topic.name}")
    private String TOPIC;

    private final KafkaTemplate<String, User> kafkaTemplate;

    @Autowired
    public ProducerModule(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    void sendMessage(User user) {
        this.kafkaTemplate.send(this.TOPIC, user.getName(), user);
        log.info(String.format("Produced user -> %s", user));
    }
}
