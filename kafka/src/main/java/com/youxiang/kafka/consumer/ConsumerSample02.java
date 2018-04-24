package com.youxiang.kafka.consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;

/**
 * @author: Rivers
 * @date: 2018/4/24
 */
public class ConsumerSample02 {

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();

    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "120.77.177.184:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "CountryCounter");

        consumer = new KafkaConsumer<String, String>(properties);
        try {
            consumer.subscribe(Collections.singletonList("CustomerCountry"), new HandleReblance());
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() +
                            ", offset = " + record.offset() + ", customer = " +
                            record.key() + ", country =" + record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                }
                consumer.commitAsync(currentOffsets, null);
            }
        } catch (WakeupException ex) {
          // do nothing
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
            }
        }
    }

    private static class HandleReblance implements ConsumerRebalanceListener {
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            System.out.println("Lost partitions in rebalance.Commiting current offset:" + currentOffsets);
            consumer.commitSync(currentOffsets);
        }

        public void onPartitionsAssigned(Collection<TopicPartition> collection) {

        }
    }
}
