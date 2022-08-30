package org.example.helpers.utils;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.helpers.kafkaPropertiesGenerator.KafkaModule;
import org.example.helpers.kafkaPropertiesGenerator.KafkaProperties;
import org.example.helpers.kafkaPropertiesGenerator.KafkaPropertiesFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    private KafkaConsumer<String, String> consumer;

    private List<String> recordsFromPoll;

    private ConsumerRecords<String, String> records;

    public KafkaConsumer getKafkaConsumer() {
        KafkaProperties properties = KafkaPropertiesFactory.getKafkaProperties(String.valueOf(KafkaModule.CONSUMER));
        consumer = new KafkaConsumer<>(properties.getProperties());

        return consumer;
    }

    public List<String> getOffsetDataFromTopic(ConsumerRecords<String, String> records) {
        recordsFromPoll = new ArrayList<>();
        records.forEach(record -> {
            recordsFromPoll.add(record.value());
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
}
