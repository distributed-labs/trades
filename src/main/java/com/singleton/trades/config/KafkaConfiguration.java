package com.singleton.trades.config;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfiguration {

  @Bean
  public KafkaStreams kafkaStreams() {
    final var props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    final var kafkaStreams = new KafkaStreams(kafkaTopology(), props);
    kafkaStreams.start();

    return kafkaStreams;
  }

  @Bean
  public Topology kafkaTopology() {
    final var streamsBuilder = new StreamsBuilder();

    streamsBuilder.stream("trades").to("trades-values");

    return streamsBuilder.build();
  }

}
