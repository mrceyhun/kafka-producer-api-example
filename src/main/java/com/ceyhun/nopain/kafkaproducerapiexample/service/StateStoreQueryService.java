package com.ceyhun.nopain.kafkaproducerapiexample.service;

import com.ceyhun.nopain.kafkaproducerapiexample.bean.FavoriteSingers;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class StateStoreQueryService {

	private final static String stateStoreName = "nopain-playlist-k-table";

	private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	public StateStoreQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
		this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
	}

	public List<Long> getAllListeners() {

		List<Long> listeners = new ArrayList<>();
		System.out.println(getReadOnlyStore().all().hasNext());
		getReadOnlyStore().all().forEachRemaining(keyValue -> listeners.add(keyValue.key));
		return listeners;
	}

	public List<String> getListenerSongs(Long listenerId) {

		return getReadOnlyStore().get(listenerId).getSingers();
	}

	public List<String> getRangeOfListenersSongs(Long listenerId1, Long listenerId2) {

		// Return set of songs with given id range of users
		Long rangeStart;
		Long rangeEnd;
		if (listenerId1 < listenerId2) {
			rangeStart = listenerId1;
			rangeEnd = listenerId2;
		} else {
			rangeStart = listenerId2;
			rangeEnd = listenerId1;
		}

		List<String> listeners = new ArrayList<>();
		getReadOnlyStore().range(rangeStart, rangeEnd)
											.forEachRemaining(keyValue -> listeners.addAll(keyValue.value.getSingers()));
		return new ArrayList<>(new HashSet<>(listeners));
	}

	private ReadOnlyKeyValueStore<Long, FavoriteSingers> getReadOnlyStore() {
		return streamsBuilderFactoryBean.getKafkaStreams().store(stateStoreName, QueryableStoreTypes.keyValueStore());
	}
}
