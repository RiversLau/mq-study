package com.youxiang.kafka.consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author: Rivers
 * @date: 2018/4/24
 */
public class ConsumerSample01 {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "120.77.177.184:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "CountryCounter");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("CustomerCountry"));

        Map<String, Object> custCountryMap = new HashMap<String, Object>();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() +
                            ", offset = " + record.offset() + ", customer = " +
                            record.key() + ", country =" + record.value());
                    int updateCount = 1;
                    if (custCountryMap.containsValue(record.value())) {
                        updateCount = (Integer)custCountryMap.get(record.value()) + 1;
                    }
                    custCountryMap.put(record.value(), updateCount);

                    JSONObject json = new JSONObject(custCountryMap);
                    System.out.println(json.toJSONString());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
