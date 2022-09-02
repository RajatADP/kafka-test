package org.ubs.helpers.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ubs.helpers.kafkaPropertiesGenerator.KafkaModule;
import org.ubs.helpers.kafkaPropertiesGenerator.KafkaProperties;
import org.ubs.helpers.kafkaPropertiesGenerator.KafkaPropertiesFactory;
import org.ubs.model.ConsumerData;
import org.ubs.model.ProducerData;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class.getSimpleName());

    private KafkaConsumer<String, String> consumer;

    private List<String> recordsFromPoll;

    private ConsumerRecords<String, String> records;

    public KafkaConsumer getKafkaConsumer() {
        KafkaProperties properties = KafkaPropertiesFactory.getKafkaProperties(String.valueOf(KafkaModule.CONSUMER));
        consumer = new KafkaConsumer<>(properties.getProperties());

        return consumer;
    }

    public List<String> getOffsetDataFromTopic() {
        recordsFromPoll = new ArrayList<>();
        records.forEach(record -> {
            if (record.value().contains("demo3")) {
                System.out.println("last offset read - " + record.value());
                recordsFromPoll.add(record.value());
            }
            consumer.commitAsync();
        });
        return recordsFromPoll;
    }


    public ConsumerRecords pollingKafkaTopic(String timeOut, String topicName) {
        records = consumer.poll(Duration.ofMillis(Long.parseLong(timeOut)));

        if (records.isEmpty())
            throw new NoRecordFoundInTopicException(topicName);
        return records;
    }

    public String convertObjectToJsonString(ProducerData producerData) {
        ObjectMapper objectMapper = new ObjectMapper();
        String strMsg = null;
        try {
            strMsg = objectMapper.writeValueAsString(producerData);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.error("*******Parsing failed*********");
        }

        logger.info("*******Parsing successful*********");
        return strMsg;
    }

    public ConsumerData convertJsonStringToObject(String jsonString) {
        ObjectMapper objectMapper = new ObjectMapper();
        ConsumerData consumerData = null;

        try {
            consumerData = objectMapper.readValue(jsonString, ConsumerData.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.error("*******Parsing failed*********");
        }

        logger.info("*******Parsing successful*********");
        return consumerData;
    }
}
