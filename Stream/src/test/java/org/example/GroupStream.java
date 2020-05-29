package org.example;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class GroupStream {

    private static final int TEMPERATURE_THRESHOLD = 20;
    private static final int TEMPERATURE_WINDOW_SIZE = 10;
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-temperature");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.216.235:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("testTopic04");

        KGroupedStream<String, String> groupBy = source.groupBy(new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value){
                JSONObject json = JSONObject.parseObject(value);
                String attacker = String.valueOf(json.get("attacker"));
                return attacker;
            }
        });
        
        KGroupedStream<String, String> groupBy60min = groupBy;
        KGroupedStream<String, String> groupBy300min = groupBy;
        KGroupedStream<String, String> groupBy1hour = groupBy;
        groupBy60min.windowedBy(TimeWindows.of(Duration.ofSeconds(TEMPERATURE_WINDOW_SIZE)))
//        groupBy10min.windowedBy(TimeWindows.of(Duration.ofSeconds(TEMPERATURE_WINDOW_SIZE)).advanceBy(Duration.ofSeconds(3)))
                .count(Materialized.as("itemGroupBy10min-state-store"))
                .toStream()
                .map((key, count) -> KeyValue.pair(key.toString().substring(1, key.toString().indexOf("@")), count.toString()+"******10min"))
                .print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-temperature-shutdown-hook"){
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
