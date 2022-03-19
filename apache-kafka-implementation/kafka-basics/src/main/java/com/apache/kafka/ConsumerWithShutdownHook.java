package com.apache.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithShutdownHook {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithShutdownHook.class);

    public static void main(String[] args) {
        log.info("Inside kafka consumer with shutdown hook!");

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-consumer-group-3";
        String offsetProperty = "earliest";
        String topic = "demo_java";

        //Set the properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetProperty);

        //Create kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown!!");
                consumer.wakeup();
                //join the main thread to allow the execution in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        try {

            //Subscribe to the topic to read data
            consumer.subscribe(Arrays.asList(topic));

            //Poll data from the topic
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Topic:" + record.topic() + " " + "Partition:" + record.partition());
                    log.info("Key:" + record.key() + " " + "Message:" + record.value());
                }
            }
        } catch (WakeupException e) {
            //ignore this known exception
            log.info("Wake up exception");
        } catch (Exception e) {
            log.error("Unexpected exception");
        } finally {
            consumer.close();
            log.info("The consumer is now gracefully closed!");
        }
    }
}
