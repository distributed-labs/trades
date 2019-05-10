package com.singleton.trades.config;

import static java.util.Collections.singletonMap;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import stock.Trade;

@Configuration
public class KafkaConfiguration {

  @Bean
  public KafkaStreams kafkaStreams() {
    final var props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
    props
        .put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put("schema.registry.url", "http://localhost:8081");

    final var serdeConfig = singletonMap(
        "schema.registry.url",
        "http://localhost:8081"
    );

    try (final var valueSerde = new SpecificAvroSerde<Trade>()) {
      valueSerde.configure(serdeConfig, false);

      final var streamsBuilder = new StreamsBuilder();
      streamsBuilder
          .stream("trades", Consumed.with(Serdes.Long(), valueSerde))
          .to("rich-trades");

      final var topology = streamsBuilder.build();

      final var kafkaStreams = new KafkaStreams(topology, props);
      kafkaStreams.start();

      return kafkaStreams;
    }
  }

}
