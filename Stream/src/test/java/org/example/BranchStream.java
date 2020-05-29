package org.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BranchStream {

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
        //拆分源
        KStream<String, String>[] branchs = source.branch(
                ((key, value) -> {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONObject threatJson = JSONObject.parseObject(String.valueOf(jsonObject.get("threat")));
                    if (threatJson != null) {
                        return true;
                    }
                    return false;
                }),
                ((key, value) -> {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONObject threatJson = JSONObject.parseObject(String.valueOf(jsonObject.get("threat")));
                    if (threatJson == null) {
                        return true;
                    }
                    return false;

                }),
                ((key, value) -> true)
        );


        branchs[0].to("c1");
        System.out.print("branchs[0] 告警流===>>>");

        branchs[1].to("c2");
        System.out.print("branchs[1] 普通流===>>>");

        branchs[2].to("c3");
        System.out.print("branchs[2] 其它流===>>>");

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
