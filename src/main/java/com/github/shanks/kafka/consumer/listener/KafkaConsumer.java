package com.github.shanks.kafka.consumer.listener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KafkaConsumer {

	private Map<String, List<KafkaStream<String, String>>> consumerMap;
	
	private final Decoder<String> keyDecoder = new StringDecoder(new VerifiableProperties());
	
	private final Decoder<String> valueDecoder = new StringDecoder(new VerifiableProperties());

	private ConsumerConnector consumerConnector;
	
	public KafkaConsumer(Properties properties, List<String> topics) {
		this.consumerConnector =  Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		if (CollectionUtils.isNotEmpty(topics)) {
			for (String topic : topics) {
				topicCountMap.put(topic, 1);
			}
		}
		consumerMap = consumerConnector.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
	}

	public KafkaStream<String, String> getKafkaStream(String topic) {
		if (MapUtils.isNotEmpty(consumerMap) && consumerMap.containsKey(topic) 
				&& CollectionUtils.isNotEmpty(consumerMap.get(topic))) {
			return consumerMap.get(topic).get(0);
		}
		return null;
	}
	
	public ConsumerConnector getConnector() {
		return this.consumerConnector;
	}
}
