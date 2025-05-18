package com.demo.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);

		KafkaStarter starter = new KafkaStarter();

		starter.startConsumer("demo-topic", "group1");
		starter.startConsumer("new-demo-topic", "group2");
		starter.startConsumer("another-topic", "group3");
		starter.startConsumer("another-topic1", "group4");
		starter.startConsumer("another-topic2", "group5");
		starter.startConsumer("another-topic3", "group6");
		starter.startConsumer("another-topic4", "group7");
		starter.startConsumer("another-topic5", "group8");
		starter.startConsumer("another-topic6", "group9");
		starter.startConsumer("another-topic7", "group0");
		starter.startConsumer("another-topic8", "group11");
		starter.startConsumer("another-topic9", "group12");
	}

}
