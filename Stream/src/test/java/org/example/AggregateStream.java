package org.example;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kafka.utils.json.JsonObject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.kstream.internals.TimeWindowedKStreamImpl;
import org.apache.kafka.streams.state.WindowStore;

import java.io.Serializable;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AggregateStream {

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

        KTable<Windowed<String>, Statistics> max = source
                .selectKey(new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value) {
                return "stat";
            }
        }).groupBy(new KeyValueMapper<String, String, String>() {
                    @Override
                    public String apply(String key, String value) {
                        JSONObject json = JSONObject.parseObject(value);
                        String atta = String.valueOf(json.get("attacker"));
                        System.out.println("atta======>>>"+atta);
                        return atta;
                    }
                })
                .windowedBy(TimeWindows.of(Duration.ofSeconds(TEMPERATURE_WINDOW_SIZE)))
                .aggregate(
                        new Initializer<Statistics>() {
                               @Override
                               public Statistics apply() {
                                   Statistics avgAndSum = new Statistics(0L, 0L, 0L, null);
                                   return avgAndSum;
                               }
                           },
                        new Aggregator<String, String, Statistics>() {
                            @Override
                            public Statistics apply(String key, String value, Statistics aggregate) {
                                System.out.println("aggKey:" + key + ",  newValue:" + value + ", aggKey:" + aggregate);
                                Long newValueLong = 0L;
                                JSONObject jsonObject = new JSONObject();

                                try {
                                    JSONObject json = JSON.parseObject(value);
                                    newValueLong = json.getLong("temp") == null ? 0L : json.getLong("temp");
                                }
                                catch (ClassCastException ex) {
                                    newValueLong = Long.valueOf("0");
                                }

                                aggregate.setCount(aggregate.getCount() +1);
                                aggregate.setSum(aggregate.getSum() + newValueLong);
                                aggregate.setAvg(aggregate.getSum() / aggregate.getCount());
                                aggregate.setJson(jsonObject);
                                return aggregate;
                            }
                        },
                        Materialized.<String, Statistics, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-temp-stream-store")
                                .withValueSerde(Serdes.serdeFrom(new StatisticsSerializer(), new StatisticsDeserializer()))
                );
//        Topology topology = builder.build();
//        KafkaStreams stream = new KafkaStreams(topology, props);
//        stream.start();

//        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
//        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(Serdes.String().deserializer(), TEMPERATURE_WINDOW_SIZE);
//        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);


        max.toStream().print(Printed.toSysOut());
//        max.toStream().to("iot-temp-stat", Produced.with(windowedSerde, Serdes.serdeFrom(new StatisticsSerializer(), new StatisticsDeserializer())));

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
