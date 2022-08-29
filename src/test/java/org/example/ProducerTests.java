package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.helpers.KafkaModule;
import org.example.helpers.KafkaProperties;
import org.example.helpers.KafkaPropertiesFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Hello world!
 */
public class ProducerTests extends BaseConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ProducerTests.class.getSimpleName());

    @Test
    void test_KafkaProducer() {
        logger.info("Starting Kafka Producer test");
        KafkaProperties properties = KafkaPropertiesFactory.getKafkaProperties(String.valueOf(KafkaModule.PRODUCER));
        producer = new KafkaProducer<>(properties.getProperties());
        ProducerRecord<String, String> record = new ProducerRecord<>(prop.getProperty("TOPIC_NAME"), "Hye Kafka");
        producer.send(record);
        logger.info("Kafka message sent!");
    }

    @AfterMethod
    private void tearDown() {
        producer.flush();
        producer.close();
    }
}
