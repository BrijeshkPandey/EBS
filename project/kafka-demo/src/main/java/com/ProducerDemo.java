package com;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.awt.X11.XSystemTrayPeer;

import java.util.Properties;

public class ProducerDemo {

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

        producer.send(producerRecord);
        producer.flush();
        producer.close();
    }
}
