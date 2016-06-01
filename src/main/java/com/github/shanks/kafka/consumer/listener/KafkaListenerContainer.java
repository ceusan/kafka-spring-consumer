package com.github.shanks.kafka.consumer.listener;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;

import kafka.consumer.KafkaStream;

public class KafkaListenerContainer implements InitializingBean {

	private final static Integer FIXED_THREAD_POOL_COUNT = 1;

	private Executor taskExecutor;

	private List<KafkaListener<?>> listeners;

	private KafkaConsumer kafkaConsumer;

	private MessageConverter messageConverte = new StringMessageConverter();

	@Override
	public void afterPropertiesSet() throws Exception {
		if (taskExecutor == null) {
			taskExecutor = Executors.newFixedThreadPool(getThreadCount());
		}
		processer();
	}

	private void processer() {
		if (CollectionUtils.isNotEmpty(listeners)) {
			for (final KafkaListener<?> listener : listeners) {
				taskExecutor.execute(new Runnable() {
					@Override
					public void run() {
						KafkaStream<String, String> kafkaStream = kafkaConsumer
								.getKafkaStream(listener.subscribeTopic());
						if (kafkaStream != null) {
							listener.receive(kafkaConsumer.getConnector(), kafkaStream, messageConverte);
						}
					}
				});
			}
		}
	}

	public Integer getThreadCount() {
		if (CollectionUtils.isNotEmpty(listeners)) {
			return listeners.size();
		}
		return FIXED_THREAD_POOL_COUNT;
	}

	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	public void setListeners(List<KafkaListener<?>> listeners) {
		this.listeners = listeners;
	}

	public void setKafkaConsumer(KafkaConsumer kafkaConsumer) {
		this.kafkaConsumer = kafkaConsumer;
	}

	public void setMessageConverte(MessageConverter messageConverte) {
		this.messageConverte = messageConverte;
	}

}
