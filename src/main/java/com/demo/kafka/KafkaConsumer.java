package com.demo.kafka;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.core.ConsumerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

public class KafkaConsumer {

    public KafkaConsumer(ConsumerFactory<String, String> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger emptyPollCount = new AtomicInteger(0);
    private static final int MAX_SLEEP_SECONDS = 2;
    private static int failedAttempts = 0;
    long lastSuccessfulOffset = -1;
    private Consumer<String, String> consumer;
    private final ConsumerFactory<String, String> consumerFactory;


    public void startConsumer(String topic, String groupId) {
        Thread thread = new Thread(() -> {
            consumer = consumerFactory.createConsumer(groupId, null, null);
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Susbcription to " + topic + " completed.");
            try {
                while (running.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    processRecords(records, topic);
                    if (!records.isEmpty()) {
                        processRecords(records, topic);
                        emptyPollCount.set(0);
                    }
                    else{
                        System.out.println("No Records Available for topic " + topic);
                        handleEmptyPoll();
                    }
                }
            } catch (Exception e) {
                //Handling the Exceptions here
            }
        },  "KafkaConsumerThread-" + topic);
        thread.start();
    }


    private void processRecords(ConsumerRecords<String, String> records, String topic) {
        Integer count = 0;
        Integer i = 0;
        Boolean failed = false;
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Data received for topic " + record.topic() + " :- " + record.value());
            i++;
            count++;
            lastSuccessfulOffset = record.offset();
            System.out.println("Record Number :- " + i);
            System.out.println("Record Offset :- " + record.offset());
            try {
                System.out.println("Processing: " + record.value());

                if(count == 2){
                    failedAttempts++;
                    failed = true;
                    throw new RuntimeException("demo");
                }
            } catch (Exception e) {
                System.err.println("Failed to process record: " + record.value());
                // break to stop processing further and retry in next poll
                break;
            }
        }

        System.out.println("Last successful offset is: " + lastSuccessfulOffset);

        TopicPartition tp = new TopicPartition(topic, 0);
        if(failed == true){
            if(failedAttempts == 3){
                consumer.seek(tp, lastSuccessfulOffset+1);
                failedAttempts = 0;
                //Integrate Raygun here
            }
            else{
                consumer.seek(tp, lastSuccessfulOffset);
            }
        }

        else{
            consumer.seek(tp, lastSuccessfulOffset+1);
            failedAttempts = 0;
        }
    }


    private void handleEmptyPoll() throws InterruptedException {
        int currentCount = emptyPollCount.incrementAndGet();
        int sleepSeconds = Math.min(currentCount, MAX_SLEEP_SECONDS);

        System.out.println("Sleeping " + sleepSeconds + "s");
        Thread.sleep(sleepSeconds * 500L);
    }

}
