package com.github.shanks.kafka.consumer;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;

import lombok.Setter;

//@Component
public class KafkaListener {

	@Setter
	private KafkaEnv kafkaEnv;

	@Autowired
	private TaskExecutor taskExecutor;

	
	@PostConstruct
	public void init() {
		KafkaConsumer helloworldConsumer = new KafkaConsumer(kafkaEnv);
		taskExecutor.execute(helloworldConsumer);
	}

}
