package com.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemowithKeys {

    // create logger instance
    private static final Logger log= LoggerFactory.getLogger(ProducerDemowithKeys.class);


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

        //sending multiple data
        for(int i=0;i<10;i++) {

            //create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("second_topic",
                    "id_"+i,"hello world "+i);

            //send the data - asynchronous
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null == e) {
                        log.info("Received new metadata " + "\n" + "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Error while sending " + e);
                    }
                }
            });

            //Add sleep to validate round robin behaviour of kafka
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // flush data
        kafkaProducer.flush();

        // close producer
        kafkaProducer.close();
    }

}
