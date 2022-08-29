package org.example.helpers;

public class KafkaPropertiesFactory {

    private static KafkaProperties properties;

    private KafkaPropertiesFactory() {
    }

    public static synchronized KafkaProperties getKafkaProperties(String filter) {

        switch (filter) {
            case "PRODUCER": {
                properties = new KafkaProducerProperties();
                break;
            }

            case "CONSUMER": {
                properties = new KafkaConsumerProperties();
                break;
            }

            default:
                throw new RuntimeException("Invalid Selection! Should be PRODUCER or CONSUMER");
        }
        return properties;
    }
}
