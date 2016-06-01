package com.github.shanks.kafka.consumer.listener;

import java.lang.reflect.ParameterizedType;

import org.springframework.messaging.converter.MessageConverter;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class KafkaListener<T> {

	
	@SuppressWarnings("unchecked")
	public void receive(ConsumerConnector connect, KafkaStream<String, String> kafkaStream, MessageConverter messageConverte) {
		T object = null;
		MessageAndMetadata<String, String> k = null;
		ConsumerIterator<String, String> iterator = kafkaStream.iterator();
		Class<T> clazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
		while (iterator.hasNext()) {
			try {
				k = kafkaStream.iterator().next();
				object = (T) messageConverte.fromMessage(new KafkaMessage(k),clazz);
				receive(object);
				log.debug("process kafkaMessage {}",new KafkaMessage(k));
				connect.commitOffsets();
			} catch (Exception e) {
				log.error(e.getMessage(), e);
				throw new RuntimeException(e);
			}
		}
		
	}
	
	public abstract String subscribeTopic();
	
	public abstract void receive(T message) throws Exception;
	
	public void afterReceive() {
		
	}
	
	public void onError() {
		
	}
	
}
