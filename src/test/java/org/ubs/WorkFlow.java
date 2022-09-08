package org.ubs;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.ubs.base.BaseConfiguration;
import org.ubs.helpers.kafkaPropertiesGenerator.KafkaModule;
import org.ubs.helpers.kafkaPropertiesGenerator.KafkaProperties;
import org.ubs.helpers.kafkaPropertiesGenerator.KafkaPropertiesFactory;
import org.ubs.model.ConsumerData;
import org.ubs.model.ProducerData;

import java.util.Collections;
import java.util.List;

public class WorkFlow extends BaseConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(WorkFlow.class.getSimpleName());
    private List<String> recordsFromPoll;
    private ConsumerData consumerData;
    private ProducerData producerData;

    private static int partitionValue = 0;

    void setUpProducerData() {
        producerData = new ProducerData(11, "demo3");
    }

    @Test(priority = 1)
    void test_SendMessageFromProducer() {
        setUpProducerData();

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
        tearDownProducer();
    }

    private void tearDownProducer() {
        producer.flush();
        producer.close();
    }

    @Test(priority = 2)
    void test_readFromConsumer() {
        logger.info("************polling**************");
        consumer = utils.getKafkaConsumer();
        TopicPartition partitionToReadFrom = new TopicPartition(prop.getProperty("TOPIC_NAME"), partitionValue);
        consumer.assign(Collections.singleton(partitionToReadFrom));

        long pos = consumer.position(partitionToReadFrom);
        if (pos > 10)
            consumer.seek(partitionToReadFrom, pos - 10);
        else
            consumer.seekToBeginning(Collections.singleton(partitionToReadFrom));

        utils.pollingKafkaTopic(prop.getProperty("POLL_TIMEOUT"), prop.getProperty("TOPIC_NAME"));
        recordsFromPoll = utils.getOffsetDataFromTopic();
        consumerData = utils.convertJsonStringToObject(recordsFromPoll.get(0).trim());

        Assert.assertEquals(consumerData.getName(), "demo3");
        logger.info("Kafka message read!" + recordsFromPoll);
    }
}


