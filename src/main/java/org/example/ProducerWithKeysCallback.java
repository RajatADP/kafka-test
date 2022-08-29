package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Hello world!
 */
public class ProducerWithKeysCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerWithKeysCallback.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Hello ProducerWithCallback!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //crate producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i<10; i++) {

            String topic = "first_topic";
            String value = "Hye Kafka " + i;
            String key = "id_" + i;

            //create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            //send
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null)
                        logger.info("Record send successfully from producer to {topic} topic \n" + recordMetadata.topic() + "\n " + recordMetadata.partition()
                                + "\n " + recordMetadata.offset() + "\n " + record.key());
                    else
                        logger.error("message not sent");
                }
            });

        }
        //flush
        producer.flush();

        //close
        producer.close();


    }
}
