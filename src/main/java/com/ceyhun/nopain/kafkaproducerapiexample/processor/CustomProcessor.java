package com.ceyhun.nopain.kafkaproducerapiexample.processor;

import com.ceyhun.nopain.kafkaproducerapiexample.bean.FavoriteSingers;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

public class CustomProcessor implements Processor<Long, FavoriteSingers> {

	private static final Integer SCHEDULE = 60; //sec

	private KeyValueStore<Long, FavoriteSingers> stateStore;

	private final String stateStoreName;

	public CustomProcessor(String stateStoreName) {
		this.stateStoreName = stateStoreName;
	}

	@Override
	public void init(ProcessorContext processorContext) {
		stateStore = (KeyValueStore<Long, FavoriteSingers>) processorContext.getStateStore(stateStoreName);

		processorContext.schedule(Duration.ofSeconds(SCHEDULE), PunctuationType.WALL_CLOCK_TIME,
															new CustomPunctuator(processorContext, stateStore));
		Objects.requireNonNull(stateStore, "State store can't be null");
	}

	@Override
	public void process(Long listenerId, FavoriteSingers favoriteSingers) {
		System.out.println("Key: " + listenerId + " Value: " + favoriteSingers.toString());
		favoriteSingers.getSingers().replaceAll(String::toUpperCase);
		if (favoriteSingers.getSingers().contains("ARCTIC MONKEYS")) {
			List<String> singers = favoriteSingers.getSingers();
			singers.replaceAll(s -> s.equals("ARCTIC MONKEYS") ? "ARCTIC MONKEYS - THE KING" : s);
		}
		System.out.println("Put: " + listenerId + " - " + favoriteSingers.toString());
		stateStore.put(listenerId, favoriteSingers);
	}

	@Override
	public void close() {

	}
}
