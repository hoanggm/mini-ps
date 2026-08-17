package org.mini.pubsub.netty.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import org.mini.pubsub.config.GlobalConfig;
import org.mini.pubsub.core.TopicManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterManager {
    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);
    private final HazelcastInstance hazelcast;
    private TopicManager topicManager;

    public ClusterManager() {
        Config config = new Config();
        config.setClusterName("mini-pubsub-cluster");

        String hzPortStr = GlobalConfig.HAZELCAST_PORT;
        int hzPort = (hzPortStr != null) ? Integer.parseInt(hzPortStr) : 5701;
        config.getNetworkConfig().setPort(hzPort).setPortAutoIncrement(false);

        String clusterMembers = GlobalConfig.CLUSTER_MEMBERS;
        if (clusterMembers != null && !clusterMembers.isBlank()) {
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            var tcpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
            tcpConfig.setEnabled(true);

            for (String member : clusterMembers.split(",")) {
                tcpConfig.addMember(member.trim());
            }
        }

        this.hazelcast = Hazelcast.newHazelcastInstance(config);
        log.info("[CLUSTER] Hazelcast-Cluster-Node initialized successfully");
    }

    public void setTopicManager(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    /**
     * Broadcast tin nhắn tới tất cả các Node trong Cluster thông qua Hazelcast ITopic
     */
    public void publishToCluster(String topic, byte[] payload) {
        ITopic<byte[]> clusterTopic = hazelcast.getTopic("pubsub:" + topic);
        clusterTopic.publish(payload);
    }

    /**
     * Lắng nghe tin nhắn từ Cluster về Topic này và đẩy xuống các Local TCP Clients
     */
    public void registerTopicListener(String topic) {
        ITopic<byte[]> clusterTopic = hazelcast.getTopic("pubsub:" + topic);

        clusterTopic.addMessageListener(message -> {
            byte[] payload = message.getMessageObject();
            if (topicManager != null) {
                // Đẩy message xuống các TCP Clients kết nối trực tiếp vào Node này
                topicManager.publishLocal(topic, payload);
            }
        });
    }

    public void shutdown() {
        if (hazelcast != null) {
            hazelcast.shutdown();
        }
    }
}
