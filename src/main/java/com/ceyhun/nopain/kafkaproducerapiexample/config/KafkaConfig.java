package com.ceyhun.nopain.kafkaproducerapiexample.config;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;

import static java.util.Map.entry;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.apache.kafka.streams.StreamsConfig.topicPrefix;

@Configuration
@EnableKafka
public class KafkaConfig {

	private String inputTopic = "example-input-topic";

	private String outputTopic = "example-output-topic";

	private String bootstrapServers = "localhost:9092";

	private String applicationId = "kafka-producer-api-application";

	@Bean
	public KafkaStreamsConfiguration kafkaStreamsConfigConfiguration() {
		String tsExtractor = WallclockTimestampExtractor.class.getName();
		return new KafkaStreamsConfiguration(
			Map.ofEntries(
				entry(APPLICATION_ID_CONFIG, applicationId),
				entry(DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, tsExtractor),
				entry(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
				entry(NUM_STREAM_THREADS_CONFIG, 1),
				entry(consumerPrefix(SESSION_TIMEOUT_MS_CONFIG), 30000),
				entry(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest"),
				// PROD CONFS
				entry(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class),
				entry(REPLICATION_FACTOR_CONFIG, 1),
				entry(CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L), // 10MB cache
				entry(topicPrefix(TopicConfig.RETENTION_MS_CONFIG), Integer.MAX_VALUE),
				entry(producerPrefix(ProducerConfig.ACKS_CONFIG), "all"),
				entry(producerPrefix(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), 2147483647),
				entry(producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 9223372036854775807L)));
	}

	@Bean
	public StreamsBuilderFactoryBeanCustomizer customizer() {
		return fb -> fb.setStateListener((newState, oldState) -> {
			System.out.println("State transition from " + oldState + " to " + newState);
		});
	}

	@Bean("nopainStreamsBuilderFactoryBean")
	@Primary
	public StreamsBuilderFactoryBean streamsBuilderFactoryBean(KafkaStreamsConfiguration kafkaStreamsConfigConfiguration)
		throws Exception {

		StreamsBuilderFactoryBean streamsBuilderFactoryBean =
			new StreamsBuilderFactoryBean(kafkaStreamsConfigConfiguration);
		streamsBuilderFactoryBean.afterPropertiesSet();
		streamsBuilderFactoryBean.setInfrastructureCustomizer(new CustomCustomizer(inputTopic, outputTopic));
		streamsBuilderFactoryBean.setCloseTimeout(30); //30 seconds
		return streamsBuilderFactoryBean;
	}
}