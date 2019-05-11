package com.singleton.trades.config;

import static java.util.Collections.singletonMap;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import stock.RichTrade;
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
      try (final var richValueSerde = new SpecificAvroSerde<RichTrade>()) {
        valueSerde.configure(serdeConfig, false);
        richValueSerde.configure(serdeConfig, false);

        final var streamsBuilder = new StreamsBuilder();
        streamsBuilder
            .stream("trades", Consumed.with(Serdes.Long(), valueSerde))
            .map((key, value) -> new KeyValue<>(
                key,
                // TODO: replace by mapper library call
                RichTrade
                    .newBuilder()
                    .setAccount(value.getAccount())
                    .setAmount(value.getAmount())
                    .setTicker(value.getTicker())
                    .setCurrency(value.getCurrency())
                    .setDateTime(value.getDateTime())
                    .setPrice(value.getPrice())
                    .setValue(value.getPrice() * value.getAmount())
                    .build()
            ))
            .to("rich-trades", Produced.with(Serdes.Long(), richValueSerde));

        final var topology = streamsBuilder.build();

        final var kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

        return kafkaStreams;
      }
    }
  }

}
