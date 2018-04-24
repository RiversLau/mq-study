package com.youxiang.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author: Rivers
 * @date: 2018/4/24
 */
public class ProducerSample02 {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "120.77.177.184:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> pr = new ProducerRecord<String, String>("CustomerCountry", "Precision Products", "Fracne");
        try {
            RecordMetadata metadata = producer.send(pr).get();
            System.out.println(metadata.offset() + ", " + metadata.partition() + ", " + metadata.topic());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
