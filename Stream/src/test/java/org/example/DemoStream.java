package org.example;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DemoStream {

    private static final int TEMPERATURE_THRESHOLD = 20;
    private static final int TEMPERATURE_WINDOW_SIZE = 5;
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-temperature");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.216.235:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("testTopic05");

        KStream<Windowed<String>, String> max = source.selectKey(new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value) {
                return "temp";
            }
        }).groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(TEMPERATURE_WINDOW_SIZE)))
                .reduce(new Reducer<String>() {
                    @Override
                    public String apply(String value1, String value2) {
                        System.out.println("value1=" + value1+ ", value2=" + value2);
//                        JSONObject json = JSON.parseObject(value1);
//                        Integer temperature = json.getInteger("temp");
                        if (Integer.parseInt(value1) > Integer.parseInt(value2)) {
//                            return temperature.toString();
                            return value1;
                        }
                        else {
                            return value2;
                        }
                    }
                })
                .toStream()
                //温度大于20
                .filter(new Predicate<Windowed<String>, String>() {
                    @Override
                    public boolean test(Windowed<String> key, String value) {
//                        System.out.println("key=" + key+ ", value=" + value);
//                        JSONObject json = JSON.parseObject(value);
//                        Integer temperature = json.getInteger("temp");
//                        return temperature > TEMPERATURE_THRESHOLD;
                        return Integer.parseInt(value) > TEMPERATURE_THRESHOLD;
                    }
                });
//        Topology topology = builder.build();
//        KafkaStreams stream = new KafkaStreams(topology, props);
//        stream.start();

//        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
//        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(Serdes.String().deserializer(), TEMPERATURE_WINDOW_SIZE);
//        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        max.print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-temperature"){
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
