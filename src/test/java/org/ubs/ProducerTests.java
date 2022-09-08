package org.ubs;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.ubs.base.BaseConfiguration;
import org.ubs.helpers.kafkaPropertiesGenerator.KafkaModule;
import org.ubs.helpers.kafkaPropertiesGenerator.KafkaProperties;
import org.ubs.helpers.kafkaPropertiesGenerator.KafkaPropertiesFactory;
import org.ubs.model.ProducerData;

/**
 * Hello world!
 */
public class ProducerTests extends BaseConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ProducerTests.class.getSimpleName());

    private ProducerData producerData;

    @BeforeMethod
    void setUp() {
        producerData = new ProducerData(11, "demo21_21"); //list
    }

    @Test
    void test_KafkaProducer() {

        //setUp();

        logger.info("Starting Kafka Producer test");
        KafkaProperties properties = KafkaPropertiesFactory.getKafkaProperties(String.valueOf(KafkaModule.PRODUCER));
        producer = new KafkaProducer<>(properties.getProperties());

        String producerDataAsJsonString = utils.convertObjectToJsonString(producerData);

        ProducerRecord<String, String> record = new ProducerRecord<>(prop.getProperty("TOPIC_NAME"), producerDataAsJsonString);
        producer.send(record, (RecordMetadata recordMetadata, Exception e) -> {
            if (e == null)
                logger.info("Record send successfully from producer to {topic} topic " + recordMetadata.topic() + " " + recordMetadata.partition()
                        + " " + recordMetadata.offset() + " " + recordMetadata.timestamp());
            else
                logger.error("message not sent with error - " + e.getMessage());
        });
        logger.info("Kafka message sent!");

        tearDown();
    }

//    @AfterMethod
    private void tearDown() {
        producer.flush();
        producer.close();
    }
}
