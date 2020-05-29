package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class StatisticsSerializer implements Serializer<Statistics> {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Statistics data) {
        try {
            return jsonMapper.writeValueAsBytes(data);
        }
        catch (Exception ex){
            System.out.println("ex=====>>>>>"+ex);
//            log.error("jsonSerialize exception.", ex);
            return null;
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Statistics data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
