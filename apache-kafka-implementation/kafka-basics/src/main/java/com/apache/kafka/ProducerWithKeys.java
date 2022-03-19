package com.apache.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {
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
        for (int i = 0; i <= 10; i++) {

            String topic = "demo_java";
            String key = "id_" + i;
            String value = "hi from" + i;

            //create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            //send data - asynchronous
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Received new metadata \n" +
                            "Topic:" + metadata.topic() + "\n" +
                            "Key:" + producerRecord.key() + "\n" +
                            "Partitions:" + metadata.partition() + "\n" +
                            "Offset:" + metadata.offset() + "\n" +
                            "Timestamp:" + metadata.timestamp() + "\n");
                } else {
                    log.error("Exception", exception.getMessage());
                }
            });

        }
        //flush data -synchronous
        kafkaProducer.flush();

        //flush and close producer
        kafkaProducer.close();
    }
}