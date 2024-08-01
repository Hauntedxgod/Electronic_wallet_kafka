package com.example.kafka.kafka;

import com.example.kafka.domain.ActualCurrencyRate;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class KafkaProducer {

    @Value("${stock.topic-name}")
    private String TOPIC_NAME;

    private final KafkaTemplate<String, String> kafkaTemplate;


    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, UUID.randomUUID().toString(), message);
        kafkaTemplate.send(record);
    }
}

