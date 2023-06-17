package com.rick.developer.kafkaavro;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/user")
public class KafkaController {
    private final ProducerModule producerModule;

    @Autowired
    KafkaController(ProducerModule producerModule) {
        this.producerModule = producerModule;
    }

    @PostMapping(value = "/publish")
    public void sendMessageToKafkaTopic(@RequestParam("name") String name, @RequestParam("age") Integer age) {
        this.producerModule.sendMessage(new User(name, age));
    }
}
