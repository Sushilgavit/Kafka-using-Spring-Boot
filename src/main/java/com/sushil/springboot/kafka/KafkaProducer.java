package com.sushil.springboot.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {
   
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void send(String topic, String payload) {
	    try {
	        kafkaTemplate.send(topic, payload);
	        LOGGER.info("Message sent successfully to topic: {}", topic);
	    } catch (Exception e) {
	        LOGGER.error("Failed to send message to topic: {}. Error: {}", topic, e.getMessage(), e);
	    }
	}

	

}