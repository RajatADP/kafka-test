package org.ubs.helpers.kafkaPropertiesGenerator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.ubs.base.BaseConfiguration;

import java.util.Properties;

public class KafkaConsumerProperties implements KafkaProperties {

    @Override
    public Properties getProperties() {
        Properties properties = new Properties();
        BaseConfiguration configuration = new BaseConfiguration();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.prop.getProperty("BOOTSTRAP_SERVERS_CONFIG_DOCKER"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configuration.prop.getProperty("GROUP_ID_CONFIG"));
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, configuration.prop.getProperty("MAX_POLL_INTERVAL_MS"));

        return properties;
    }
}
