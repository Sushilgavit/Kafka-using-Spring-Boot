package com.sushil.springboot.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import com.sushil.springboot.forJsonDTO.User;

@Service
public class JsonKafkaProducer {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaProducer.class);

	@Autowired
	private KafkaTemplate<String, User> kafkaTemplate;

	public void sendMessage(User data) {
	    try {
	        LOGGER.info(String.format("Message to be sent -> %s", data.toString()));
	        
	        Message<User> message = MessageBuilder
	                .withPayload(data)
	                .setHeader(KafkaHeaders.TOPIC, "test-topic-json")
	                .build();
	        
	        kafkaTemplate.send(message);
	        LOGGER.info("Message sent successfully to topic: sushil");
	    } catch (Exception e) {
	        LOGGER.error("Failed to send message to topic: sushil. Error: {}", e.getMessage(), e);
	    }
	}

	
	
}
