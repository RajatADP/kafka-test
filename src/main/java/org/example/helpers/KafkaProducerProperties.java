package org.example.helpers;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.BaseConfiguration;

import java.util.Properties;

public class KafkaProducerProperties implements KafkaProperties {
    @Override
    public Properties getProperties() {
        Properties properties = new Properties();
        BaseConfiguration configuration = new BaseConfiguration();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.prop.getProperty("BOOTSTRAP_SERVERS_CONFIG"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
