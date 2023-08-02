package com.yora.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;

import com.yora.kafka.PubSubApplication;

@Configuration
public class Listners {

    private final Logger logger = LoggerFactory.getLogger(PubSubApplication.class);

    @RetryableTopic(attempts = "5", backoff = @Backoff(delay = 1000, maxDelay = 1000, multiplier = 2))
    @KafkaListener(id = "${spring.kafka.id}", topics = "price", groupId = "${spring.kafka.groupid}")
    public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
	    @Header(KafkaHeaders.GROUP_ID) String groupid, @Header(KafkaHeaders.OFFSET) long offset) {

	this.logger.info("Received: {} from {} @ {} groupid {}", in, topic, offset, groupid);
	if (in.startsWith("fail")) {
	    throw new RuntimeException("failed");
	}
    }

    @DltHandler
    public void listenDlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
	    @Header(KafkaHeaders.OFFSET) long offset) {

	this.logger.info("DLT Received: {} from {} @ {}", in, topic, offset);
    }

}
