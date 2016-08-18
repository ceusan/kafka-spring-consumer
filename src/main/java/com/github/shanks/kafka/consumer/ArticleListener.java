package com.github.shanks.kafka.consumer;

import org.springframework.beans.factory.annotation.Value;

import com.github.shanks.kafka.consumer.listener.KafkaListener;
import com.github.shanks.kafka.msg.ArticleModel;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ArticleListener extends KafkaListener<ArticleModel> {

	@Value("${article.topic}")
	@Getter
	@Accessors(fluent = true)
	private String subscribeTopic;
	
	@Override
	public void receive(ArticleModel message) throws Exception {
		log.info("ArticleListener {}", message);
	}

}
