package com;

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

public class ConsumerDemo
{
    public static void main(String[] args) {

        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class);

        System.out.println("Hello Consumer");
        String bootStrapServers="127.0.0.1:9092";
        String groupId="my-first-application";
        String topic="first_topic";

        ///create producer properties


        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        //subscrib consumer to our topic
        consumer.subscribe(Arrays.asList(topic));

       //poll fon n ew data
        while(true){
            ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record:records){
                logger.info("Key:"+ record.key()+ ", value" + record.value());
                logger.info("Partition:"+ record.partition()+ ", offset" + record.offset());
            }
        }


    }
}

