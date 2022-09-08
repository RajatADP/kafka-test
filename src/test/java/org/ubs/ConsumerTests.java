package org.ubs;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.ubs.base.BaseConfiguration;
import org.ubs.model.ConsumerData;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Hello world!
 */
public class ConsumerTests extends BaseConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerTests.class.getSimpleName());

    private List<String> recordsFromPoll;

    private ConsumerData consumerData;

    @BeforeMethod
    void create_KafkaConsumer() throws IOException {
        consumer = utils.getKafkaConsumer();


    }

    //@Test(priority = 1)
    void test_readFromConsumer() {

        logger.info("************polling**************");

        TopicPartition partitionToReadFrom = new TopicPartition(prop.getProperty("TOPIC_NAME"), 0);

        consumer.assign(Collections.singleton(partitionToReadFrom));


        consumer.seek(partitionToReadFrom, 37);




        utils.pollingKafkaTopic(prop.getProperty("POLL_TIMEOUT"), prop.getProperty("TOPIC_NAME"));



    }




    //@Test
    void test_KafkaConsumerReadsMessageFromLatestOffset() {
        logger.info("************Starting test_KafkaConsumerReadsStringMessageFromLatestOffset************");

        //subscribe to 1 topic
        consumer.subscribe(Collections.singleton(prop.getProperty("TOPIC_NAME")));

        logger.info("************polling**************");
        utils.pollingKafkaTopic(prop.getProperty("POLL_TIMEOUT"), prop.getProperty("TOPIC_NAME"));

        recordsFromPoll = utils.getOffsetDataFromTopic();

        consumerData = utils.convertJsonStringToObject(recordsFromPoll.get(0).trim());

        Assert.assertEquals(consumerData.getName(), "demo_111");
        logger.info("Kafka message read!" + recordsFromPoll);
    }

    @AfterMethod
    void tearDown() {
        consumer.close();
    }
}

//TODO
//1. reading from last offset ----- done
//2. send and receive json payload from topic


//dockerFile
//docker-compose.yml
//integrate some UI before running consumer test which will producer message in topic
//jenkins
//test case integrate in azure (explore)
//