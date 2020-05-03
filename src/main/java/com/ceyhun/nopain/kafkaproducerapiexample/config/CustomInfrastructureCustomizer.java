package com.ceyhun.nopain.kafkaproducerapiexample.config;

import com.ceyhun.nopain.kafkaproducerapiexample.bean.FavoriteSingers;
import com.ceyhun.nopain.kafkaproducerapiexample.processor.CustomProcessor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * @author ceyhunuzunoglu
 */
public class CustomInfrastructureCustomizer implements KafkaStreamsInfrastructureCustomizer {

  private static final Serde<Long> KEY_SERDE = Serdes.Long();

  private static final Serde<FavoriteSingers> VALUE_SERDE = new JsonSerde<>(FavoriteSingers.class).ignoreTypeHeaders();

  private static final String SINGERS_K_TABLE_NAME = "nopain-singers-k-table";

  private static final Deserializer<Long> KEY_JSON_DE = new JsonDeserializer<>(Long.class);

  private static final Deserializer<FavoriteSingers> VALUE_JSON_DE =
      new JsonDeserializer<>(FavoriteSingers.class).ignoreTypeHeaders();

  private final String inputTopic;

  private final String outputTopic;

  CustomInfrastructureCustomizer(String inputTopic, String outputTopic) {
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
  }

  @Override
  public void configureBuilder(StreamsBuilder builder) {
    StoreBuilder<KeyValueStore<Long, FavoriteSingers>> stateStoreBuilder =
        Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(SINGERS_K_TABLE_NAME), KEY_SERDE, VALUE_SERDE);

    Topology topology = builder.build();
    topology.addSource("Source", KEY_JSON_DE, VALUE_JSON_DE, inputTopic)
            .addProcessor("Process", () -> new CustomProcessor(SINGERS_K_TABLE_NAME), "Source")
            .addStateStore(stateStoreBuilder, "Process")
            .addSink("Sink", outputTopic, "Process");
  }
}
