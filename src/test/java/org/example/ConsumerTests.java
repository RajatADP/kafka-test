package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.helpers.KafkaModule;
import org.example.helpers.KafkaProperties;
import org.example.helpers.KafkaPropertiesFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Hello world!
 */
public class ConsumerTests extends BaseConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerTests.class.getSimpleName());

    @Test
    void test_KafkaConsumer() {
        logger.info("Starting Kafka Consumer test");
        KafkaProperties properties = KafkaPropertiesFactory.getKafkaProperties(String.valueOf(KafkaModule.CONSUMER));
        consumer = new KafkaConsumer<>(properties.getProperties());
        consumer.subscribe(Collections.singleton(prop.getProperty("TOPIC_NAME")));


        List<String> recordsFromPoll = new ArrayList<>();

        ConsumerRecords<String, String> records;

        //while (true) {
            logger.info("polling");
            records = consumer.poll(Duration.ofMillis(1000));
            logger.info("polling after consumer");

            records.forEach(s -> {
                logger.info("key, value, partition, offset " + s.key() + " "
                        + s.value() + " " + s.partition() + " " + s.offset());

                recordsFromPoll.add(s.value());
            });
            //break;

        //}

        logger.info("Kafka message read!" + recordsFromPoll);

        //Assert.assertSame(recordsFromPoll.get(0).trim(), "Hye Kafka");
    }
}

//TODO
//1. reading from last offset
//2. assuming 1 partition
//Consumer groups reading
//reading from offset