package com.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    // create logger instance
    private static final Logger log= LoggerFactory.getLogger(ProducerDemo.class);


    public static void main(String[] args) {

        //Mentioning GLobal variable
        String bootStrapServer="127.0.0.1:9092";

        //create producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create kafka producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(producerProperties);

        //create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("second_topic", "hello world!!");

        //send the data - asynchronous
        kafkaProducer.send(producerRecord);

        // flush data
        kafkaProducer.flush();

        // close producer
        kafkaProducer.close();
    }

}
