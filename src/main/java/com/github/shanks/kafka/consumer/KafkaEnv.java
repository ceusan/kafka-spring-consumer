package com.github.shanks.kafka.consumer;

import java.util.Properties;

import lombok.Data;

@Data
public class KafkaEnv {

	private String topic;
	
	private Properties properties;
	
}
