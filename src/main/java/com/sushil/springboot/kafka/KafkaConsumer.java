package com.sushil.springboot.kafka;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.sushil.springboot.forJsonDTO.User;

@Service
public class KafkaConsumer {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

	@KafkaListener(topics = "test-topic", groupId = "spring-kafka-poc-group")
	public void listen(String message) {
	    try {
	        LOGGER.info(String.format("Message received -> %s", message));
	      
	    } catch (Exception e) {
	        
	        LOGGER.error("Error occurred while processing the message: {}", e.getMessage(), e);
	    }
	}
	
	@KafkaListener(topics = "test-topic-json", groupId = "spring-kafka-poc-group")
	public void listenJson(User user) {
	    try {
	        LOGGER.info(String.format("Message received -> %s",user));
	      
	    } catch (Exception e) {
	        
	        LOGGER.error("Error occurred while processing the message: {}", e.getMessage(), e);
	    }
	}

	
}