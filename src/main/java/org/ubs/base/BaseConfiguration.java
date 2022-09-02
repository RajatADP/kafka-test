package org.ubs.base;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.ubs.helpers.utils.Utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class BaseConfiguration {
    public static Properties prop;
    public KafkaProducer<String, String> producer;

    public KafkaConsumer<String, String> consumer;

    public Utils utils = new Utils();

    public int partition = 0;

    static {
        try {
            prop = readPropertiesFile("src/main/java/resources/config.properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties readPropertiesFile(String fileName) throws IOException {
        FileInputStream fis = null;
        Properties prop = null;
        try {
            fis = new FileInputStream(fileName);
            prop = new Properties();
            prop.load(fis);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            fis.close();
        }
        return prop;
    }
}

