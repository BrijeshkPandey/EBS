package com;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallback {
  private static final  Logger logger= LoggerFactory.getLogger(ProducerDemoCallback.class);
    public static void main(String[] args) {
        System.out.println("Helllo world");

        String bootStrapServers="127.0.0.1:9092";

        ///create producer properties


        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //crate produccer
        KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);

        //creatre producer record
        ProducerRecord<String, String> producerRecord=new ProducerRecord<>("first_topic", "Hello World");

        //send data - asynchronous
/// call back demo
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("Received new meta dtata.  \n" +
                            "Topoic:" + recordMetadata.topic() + "\n" +
                            "Partioin:" + recordMetadata.partition() + "\n" +
                            "offset: " + recordMetadata.offset() + "\n" +
                            "TimeStamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while prodiuing", e);
                }
            }
        });
        producer.flush();
        producer.close();

    }
}
