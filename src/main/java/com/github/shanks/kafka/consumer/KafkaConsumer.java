package com.github.shanks.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.shanks.kafka.msg.MessageModel;
import com.github.shanks.kafka.msg.json.JsonUtils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaConsumer implements Runnable {
	
	private final KafkaStream<String, String> kafkaStream;
	
	private final Decoder<String> keyDecoder = new StringDecoder(new VerifiableProperties());
	
	private final Decoder<String> valueDecoder = new StringDecoder(new VerifiableProperties());

	private final ConsumerConnector consumerConnector;
	
	public KafkaConsumer(KafkaEnv env) {
		this.consumerConnector =  Consumer.createJavaConsumerConnector(new ConsumerConfig(env.getProperties()));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(env.getTopic(), 1);
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
		this.kafkaStream = consumerMap.get(env.getTopic()).get(0);
	}
	
	@Override
	public void run() {
		ConsumerIterator<String, String> iterator = kafkaStream.iterator();
		MessageAndMetadata<String, String> messageAndMetadata = null;
		while (iterator.hasNext()) {
			try {
				messageAndMetadata = iterator.next();
				log.info("consumer process message {}, partition {}, offset {}", JsonUtils.parse(messageAndMetadata.message(), MessageModel.class), messageAndMetadata.partition(), messageAndMetadata.offset());
			} catch (Exception e) {
				log.error("process error : message {}", JsonUtils.parse(messageAndMetadata.message(), MessageModel.class));
			}
			
		}
	}

}
