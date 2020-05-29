package org.oppo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class TestStream {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.216.235:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

//        final Serde<String> stringSerde = Serdes.String();
//        final Serde<Long> longSerde = Serdes.Long();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("testTopic03");
        System.out.println(textLines);

        textLines.selectKey(new KeyValueMapper<String, String, Object>() {
            @Override
            public Object apply(String key, String value) {
                System.out.println("key=====>>>>> "+key+"value=====>>>>> "+value);
//                KeyValue keyValue = new KeyValue<>(key, value);
//                System.out.println("=====keyValue=====>>>>>"+keyValue);
                return value.substring(0, 10);
            }
        }).filter(((key, value) -> {
            System.out.println("filter key=====>>>>>"+key+"value====="+value);
            return false;
        })).print(Printed.toSysOut());
//.windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
        Topology topology = builder.build();
        KafkaStreams stream = new KafkaStreams(topology, props);
        stream.start();
    }
}
