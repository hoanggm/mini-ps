package org.mini.pubsub.app;

import org.mini.pubsub.config.GlobalConfig;
import org.mini.pubsub.core.TopicManager;
import org.mini.pubsub.netty.NettyServer;
import org.mini.pubsub.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class AppRunner {
    private static final Logger log = LoggerFactory.getLogger(AppRunner.class);

    public static void run() {
        log.info("Initializing Mini Pub-Sub Server");
        log.info("Date: {}", CommonUtil.formatDate(LocalDateTime.now(),
                "yyyy-MM-dd HH:mm:ss"));

        // 1. Khởi tạo Core State Manager
        TopicManager topicManager = new TopicManager(log);

        // 2. Khởi tạo Netty Server
        NettyServer server = new NettyServer(GlobalConfig.PORT, topicManager, log);

        try {
            // 3. Start Server
            server.start();

            // 4. Giữ main thread sống để đợi server dừng
            server.awaitClose();
        } catch (InterruptedException e) {
            log.error("Application interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Application fatal error", e);
        }
    }
}
