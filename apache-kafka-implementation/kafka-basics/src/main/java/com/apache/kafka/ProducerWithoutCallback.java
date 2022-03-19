package com.apache.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithoutCallback {

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        log.info("This is producer demo");

        //create the producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create a producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "demo message");

        //send data - asynchronous
        kafkaProducer.send(producerRecord);
        
        //flush data -synchronous
        kafkaProducer.flush();

        //flush and close producer
        kafkaProducer.close();
    }
}
