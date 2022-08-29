package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Hello world!
 */
public class ProducerWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Hello ProducerWithCallback!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //crate producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hye Kafka with callback");

        //send
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null)
                    logger.info("Record send successfully from producer to {topic} topic " + recordMetadata.topic() + " " + recordMetadata.partition()
                    + " " + recordMetadata.offset() + " " + recordMetadata.timestamp());
                else
                    logger.error("message not sent");
            }
        });


        //flush
        producer.flush();

        //close
        producer.close();


    }
}
