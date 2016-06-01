package com.github.shanks.kafka.consumer;

import org.springframework.beans.factory.annotation.Value;

import com.github.shanks.kafka.consumer.listener.KafkaListener;
import com.github.shanks.kafka.msg.MessageModel;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HelloListener extends KafkaListener<MessageModel> {

	@Value("${topic.hello}")
	private String topic;
	
	@Override
	public void receive(MessageModel message) throws Exception {
		log.info("HelloListener {}", message);
	}

	@Override
	public String subscribeTopic() {
		return topic;
	}


}
