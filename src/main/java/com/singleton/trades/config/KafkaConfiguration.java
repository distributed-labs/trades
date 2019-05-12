package com.singleton.trades.config;

import static java.util.Collections.singletonMap;

import com.singleton.trades.processor.TradeProcessor;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
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

    final var amountCounter = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("AmountCounter"), Serdes.Integer(), Serdes.Double()
    );

    final var averageCounter = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("AverageCounter"), Serdes.Integer(), Serdes.Double()
    );

    final var positions = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("Positions"), Serdes.Integer(), Serdes.String()
    );

    try (final var valueSerde = new SpecificAvroSerde<Trade>()) {
      try (final var richValueSerde = new SpecificAvroSerde<RichTrade>()) {
        valueSerde.configure(serdeConfig, false);
        richValueSerde.configure(serdeConfig, false);

        final var topology = new Topology();
        topology
            .addSource(
                "Source",
                Serdes.Long().deserializer(),
                valueSerde.deserializer(),
                "trades"
            )
            .addProcessor("Processor", TradeProcessor::new, "Source")
            .addStateStore(amountCounter, "Processor")
            .addStateStore(averageCounter, "Processor")
            .addStateStore(positions, "Processor")
            .addSink(
                "Destination",
                "rich-trades",
                Serdes.Long().serializer(),
                richValueSerde.serializer(),
                "Processor"
            );

        final var kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

        return kafkaStreams;
      }
    }
  }

}
