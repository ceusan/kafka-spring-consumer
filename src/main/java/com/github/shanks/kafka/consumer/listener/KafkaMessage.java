package com.github.shanks.kafka.consumer.listener;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import kafka.message.MessageAndMetadata;
import lombok.Data;

@Data
public class KafkaMessage implements Message<String> {

	private String content;

	private Long offset;
	
	private Integer partition;
	
	public KafkaMessage(MessageAndMetadata<String, String> messageAndData) {
		this.content = messageAndData.message();
		this.partition = messageAndData.partition();
		this.offset = messageAndData.offset();
	}

	@Override
	public String getPayload() {
		return content;
	}

	@Override
	public MessageHeaders getHeaders() {
		return null;
	}
	
}
