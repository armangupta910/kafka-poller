package com.demo.kafka;

import org.springframework.kafka.core.ConsumerFactory;

public class KafkaStarter {

    private KafkaConsumerConfig demo;
    private ConsumerFactory<String, String> consumerFactory;
    public KafkaStarter(){
        demo = new KafkaConsumerConfig();
        consumerFactory  = demo.consumerFactory();
    }

    public void startConsumer(String topic, String groupId){
        KafkaConsumer kafkaConsumer = new KafkaConsumer(consumerFactory);
        kafkaConsumer.startConsumer(topic, groupId);
    }
    
}
