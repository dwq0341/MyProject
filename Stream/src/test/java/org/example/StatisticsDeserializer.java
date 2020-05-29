package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class StatisticsDeserializer implements Deserializer<Statistics> {
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Statistics deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        else {
            try {
                return jsonMapper.readValue(data, Statistics.class);
            }
            catch (Exception ex){
                System.out.println("ex=====>>>>>"+ex);
//                log.error("jsonSerialize exception.", ex);
                return null;
            }
        }
    }

    @Override
    public Statistics deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}
