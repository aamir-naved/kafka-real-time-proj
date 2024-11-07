package com.aamir.springboot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    @KafkaListener(topics = "wikimedia_topic", groupId = "myGroup")
    public void consume(String message){

        log.info("Event Message Consumed: " + message);
    }
}
