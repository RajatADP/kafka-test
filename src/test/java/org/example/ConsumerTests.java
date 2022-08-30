package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

/**
 * Hello world!
 */
public class ConsumerTests extends BaseConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerTests.class.getSimpleName());

    private List<String> recordsFromPoll;

    private ConsumerRecords<String, String> records;

    private KafkaConsumer<String, String> consumer;

    @BeforeMethod
    void create_KafkaConsumer() {
        consumer = utils.getKafkaConsumer();
    }

    @Test
    void test_KafkaConsumerReadsMessageFromLatestOffset() {
        logger.info("************Starting test_KafkaConsumerReadsStringMessageFromLatestOffset************");

        //subscribe to 1 topic
        consumer.subscribe(Collections.singleton(prop.getProperty("TOPIC_NAME")));

        logger.info("************polling**************");
        records = utils.pollingKafkaTopic(prop.getProperty("POLL_TIMEOUT"), prop.getProperty("TOPIC_NAME"));

        recordsFromPoll = utils.getOffsetDataFromTopic(records);
        Assert.assertEquals(recordsFromPoll.get(0).trim(), "abc");
        //logger.info("Kafka message read!" + recordsFromPoll);
    }

    @AfterMethod
    void tearDown() {
        consumer.close();
    }
}



/*logger.info("key, value, partition, offset " + s.key() + " "
        + s.value() + " " + s.partition() + " " + s.offset());*/

//TODO
//1. reading from last offset ----- done

//replicate
//1. read till offset x and commit
//2. consumer down
//3. publish new message
//4. consumer up and start reading


//2. assuming 1 partition
//Consumer groups reading
//reading from offset