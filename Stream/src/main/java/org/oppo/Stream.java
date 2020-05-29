package org.oppo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class Stream {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.216.235:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

//        final Serde<String> stringSerde = Serdes.String();
//        final Serde<Long> longSerde = Serdes.Long();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("mytopic1");
        System.out.println(textLines);

        KTable<String, Long> wordCounts = textLines
                .flatMapValues( textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .selectKey((key, world) -> world)
                .groupByKey()
                .count(Materialized.as("counts-store"))
                .filter((k,v) -> k == "scala");

        wordCounts.toStream().to("testTopic02", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        KafkaStreams stream = new KafkaStreams(topology, props);
        stream.start();
    }
}
