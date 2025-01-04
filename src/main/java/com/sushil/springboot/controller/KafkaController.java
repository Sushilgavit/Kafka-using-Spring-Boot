package com.sushil.springboot.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sushil.springboot.forJsonDTO.User;
import com.sushil.springboot.kafka.JsonKafkaProducer;
import com.sushil.springboot.kafka.KafkaProducer;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private KafkaProducer producer;

    @Autowired
    private JsonKafkaProducer jsonKafkaProducer;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaController.class);

    @PostMapping("/send")
    public ResponseEntity<String> send(@RequestBody  String message) {
        try {
            producer.send("test-topic", message);
            LOGGER.info("Message sent successfully to topic: test-topic");
            return ResponseEntity.ok("Message sent!");
        } catch (Exception e) {
            LOGGER.error("Exception while sending message to topic: test-topic. Error: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to send message: " + e.getMessage());
        }
    }

    @PostMapping("/sendUserData")
    public ResponseEntity<String> sendUserData(@RequestBody User user) {
        try {
            jsonKafkaProducer.sendMessage(user);
            LOGGER.info("JSON message sent successfully for user: {}", user);
            return ResponseEntity.ok("JSON message sent!");
        } catch (Exception e) {
            LOGGER.error("Exception while sending JSON message: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to send JSON message: " + e.getMessage());
        }
    }
}
