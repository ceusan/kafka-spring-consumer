package com.github.shanks.kafka.consumer;

import org.springframework.beans.factory.annotation.Value;

import com.github.shanks.kafka.consumer.listener.KafkaListener;
import com.github.shanks.kafka.msg.MessageModel;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HelloListener extends KafkaListener<MessageModel> {

	@Value("${topic.hello}")
	@Getter
	@Accessors(fluent = true)
	private String subscribeTopic;
	
	@Override
	public void receive(MessageModel message) throws Exception {
		log.info("HelloListener {}", message);
	}

}
