package com.youxiang.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * @author: Rivers
 * @date: 2018/4/24
 */
public class ConsumerSample03 {

    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "120.77.177.184:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "CountryCounter");

        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("CustomerCountry"), new SaveOffsetsOnRebalance());
        consumer.poll(0);

        for (TopicPartition partition : consumer.assignment()) {
            consumer.seek(partition, getOffsetFromDB(partition));
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                processRecord(record);
                storeRecordInDB(record);
                storeOffsetInDb(record.topic(), record.partition(), record.offset());
            }
            commitDBTransaction();
        }
    }

    private static void commitDBTransaction() {
        //保存偏移量到数据库
    }

    private static long getOffsetFromDB(TopicPartition partition) {
        //根据分区获取该分区中数据库中保存的偏移量值
        return 0L;
    }

    private static void processRecord(ConsumerRecord record) {
        //处理记录
    }

    private static void storeRecordInDB(ConsumerRecord record) {
        //存储
    }

    private static void storeOffsetInDb(String topic, int partition, long offset) {

    }

    private static class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            commitDBTransaction();
        }

        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            for (TopicPartition partition : collection) {
                consumer.seek(partition, getOffsetFromDB(partition));
            }
        }
    }
}
