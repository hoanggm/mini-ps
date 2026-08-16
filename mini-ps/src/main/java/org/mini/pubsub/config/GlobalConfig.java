package org.mini.pubsub.config;

import java.io.InputStream;
import java.util.Properties;

public class GlobalConfig {
    public static Integer PORT;
    public static Integer MAX_RETRIES;
    public static Integer ACK_TIMEOUT_SECONDS;

    static {
        Properties prop = new Properties();
        try (InputStream input = GlobalConfig.class.getResourceAsStream("/application.properties")) {
            prop.load(input);
            PORT = System.getenv("PORT") != null
                    ? Integer.valueOf(System.getenv("PORT"))
                    : Integer.valueOf(prop.getProperty("server.port"));
            MAX_RETRIES = System.getenv("MAX_RETRIES") != null
                    ? Integer.valueOf(System.getenv("MAX_RETRIES"))
                    : Integer.valueOf(prop.getProperty("max-retries.count"));
            ACK_TIMEOUT_SECONDS = System.getenv("ACK_TIMEOUT_SECONDS") != null
                    ? Integer.valueOf(System.getenv("ACK_TIMEOUT_SECONDS"))
                    : Integer.valueOf(prop.getProperty("ack.timeout.seconds"));
        } catch (Exception ex) {
            PORT = 80;
            MAX_RETRIES = 3;
            ACK_TIMEOUT_SECONDS = 5;
        }
    }
}
