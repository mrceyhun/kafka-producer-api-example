package com.ceyhun.nopain.kafkaproducerapiexample.processor;

import com.ceyhun.nopain.kafkaproducerapiexample.bean.FavoriteSingers;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * @author ceyhunuzunoglu
 */
public class CustomPunctuator implements Punctuator {

  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static final Integer DELETE_X_DAYS_BEFORE = 5; // Delete from state-store

  private static final List<String> GOODIES = Arrays.asList("ZEKI MUREN", "TAYLOR SWIFT", "MAROON 5");

  private final ProcessorContext context;

  private final KeyValueStore<Long, FavoriteSingers> stateStore;

  CustomPunctuator(ProcessorContext context,
                   KeyValueStore<Long, FavoriteSingers> stateStore) {
    this.context = context;
    this.stateStore = stateStore;
  }

  @Override
  public void punctuate(long l) {
    System.out.println("Punctuator started.");

    KeyValueIterator<Long, FavoriteSingers> iter = stateStore.all();
    Date delete_before_date = Date.from(LocalDateTime.now()
                                                     .minusDays(DELETE_X_DAYS_BEFORE)
                                                     .atZone(ZoneId.of("Europe/Istanbul"))
                                                     .toInstant());

    List<Long> goodieListeners = new ArrayList<>();
    while (iter.hasNext()) {
      KeyValue<Long, FavoriteSingers> entry = iter.next();
      try {

        // If date is older than 5 days, delete with schedule which run every 60 seconds for test.
        if (DATE_FORMAT.parse(entry.value.getDate()).before(delete_before_date)) {
          stateStore.delete(entry.key);
        } else {
          stateStore.put(entry.key, entry.value);
          if (GOODIES.stream().anyMatch(new HashSet<>(entry.value.getSingers())::contains)) {
            goodieListeners.add(entry.key);
          }
        }
      } catch (ParseException e) {
        System.out.println("ERROR: " + entry.toString());
        System.out.println(e.toString());
      }
    }
    iter.close();
    retrieve(goodieListeners);

    // commit the current processing progress
    context.commit();
  }

  private void retrieve(List<Long> listeners) {
    if (!listeners.isEmpty()) {
      System.out.println("Punctuator result, listeners size : " + listeners.size());
      System.out.println(listeners.toString());
    } else {
      System.out.println("Punctuator result: EMPTY");
    }
  }
}
