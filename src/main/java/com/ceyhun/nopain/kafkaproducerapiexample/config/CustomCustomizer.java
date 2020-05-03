package com.ceyhun.nopain.kafkaproducerapiexample.config;

import com.ceyhun.nopain.kafkaproducerapiexample.bean.FavoriteSingers;
import com.ceyhun.nopain.kafkaproducerapiexample.processor.CustomProcessor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

public class CustomCustomizer implements KafkaStreamsInfrastructureCustomizer {

	private static final Serde<Long> KEY_SERDE = Serdes.Long();

	private static final Serde<FavoriteSingers> VALUE_SERDE = new JsonSerde<>(FavoriteSingers.class).ignoreTypeHeaders();

	private static final String PLAYLIST_K_TABLE = "nopain-playlist-k-table";

	private static final Deserializer<Long> KEY_JSON_DE = new JsonDeserializer<>(Long.class);

	private static final Deserializer<FavoriteSingers> VALUE_JSON_DE =
		new JsonDeserializer<>(FavoriteSingers.class).ignoreTypeHeaders();

	private final String inputTopic;

	private final String outputTopic;

	CustomCustomizer(String inputTopic, String outputTopic) {
		this.inputTopic = inputTopic;
		this.outputTopic = outputTopic;
	}

	@Override
	public void configureBuilder(StreamsBuilder builder) {
		StoreBuilder<KeyValueStore<Long, FavoriteSingers>> stateStoreBuilder =
			Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(PLAYLIST_K_TABLE), KEY_SERDE, VALUE_SERDE);

		builder.build()
					 .addSource("Source", KEY_JSON_DE, VALUE_JSON_DE, inputTopic)
					 .addProcessor("Process", () -> new CustomProcessor(PLAYLIST_K_TABLE), "Source")
					 .addStateStore(stateStoreBuilder, "Process")
					 .addSink("Sink", outputTopic, "Process");
	}
}
