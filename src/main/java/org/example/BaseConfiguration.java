package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.example.helpers.utils.Utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class BaseConfiguration {
    public static Properties prop;
    public KafkaProducer<String, String> producer;

    public Utils utils = new Utils();



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

