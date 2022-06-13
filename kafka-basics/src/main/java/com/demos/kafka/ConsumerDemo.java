package com.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    // create logger instance
    private static final Logger log= LoggerFactory.getLogger(ConsumerDemo.class);


    public static void main(String[] args) {

        //Mentioning GLobal variable
        String bootStrapServer="127.0.0.1:9092";
        String topic="second_topic";

        //create consumer properties
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my_first_group");

        //create kafka producer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);

        // subscribe consumer to our topic(s)
        kafkaConsumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while(true){
            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records){
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }

    }

}
