package org.mini.pubsub.core;

import com.google.protobuf.UnsafeByteOperations;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import org.mini.pubsub.config.GlobalConfig;
import org.mini.pubsub.netty.cluster.ClusterManager;
import org.mini.pubsub.proto.PubSubProto.MessageResponse;
import org.mini.pubsub.util.SnowflakeIdGenerator;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class TopicManager {
    private final Logger log;
    private final Map<String, Set<Subscription>> topicSubscriptions;
    private final Map<String, ChannelGroup> topicChannelGroups;
    private final Map<String, UnAckedMessage> pendingUnAckedMessages;
    private ClusterManager clusterManager;
    private SnowflakeIdGenerator idGenerator;

    public TopicManager(Logger logger) {
        this.log = logger;
        this.topicSubscriptions = new ConcurrentHashMap<>();
        this.topicChannelGroups = new ConcurrentHashMap<>();
        this.pendingUnAckedMessages = new ConcurrentHashMap<>();
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setIdGenerator(SnowflakeIdGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    /**
     * Đăng ký một Channel vào Topic.
     */
    public void subscribe(String topic, Channel channel) {
        Subscription subscription = new Subscription(topic, channel);

        // 1. Thêm vào Set Subscriptions
        Set<Subscription> subscriptions = topicSubscriptions.computeIfAbsent(
                topic, k -> ConcurrentHashMap.newKeySet()
        );
        boolean added = subscriptions.add(subscription);

        // 2. Thêm vào Netty ChannelGroup
        ChannelGroup group = topicChannelGroups.computeIfAbsent(
                topic, k -> {
                    if (clusterManager != null) {
                        clusterManager.registerTopicListener(k);
                    }
                    return new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
                }
        );
        group.add(channel);

        if (added) {
            log.info("New subscription added: {}", subscription);
        }
    }

    /**
     * Hủy đăng ký một Channel khỏi Topic.
     */
    public void unsubscribe(String topic, Channel channel) {
        Set<Subscription> subscriptions = topicSubscriptions.get(topic);
        if (subscriptions != null) {
            subscriptions.removeIf(sub -> sub.getChannel().id().equals(channel.id()));
            if (subscriptions.isEmpty()) {
                topicSubscriptions.remove(topic);
            }
        }

        ChannelGroup group = topicChannelGroups.get(topic);
        if (group != null) {
            group.remove(channel);
            if (group.isEmpty()) {
                topicChannelGroups.remove(topic);
            }
        }

        log.info("Channel [{}] unsubscribed from topic [{}]", channel.remoteAddress(), topic);
    }

    /**
     * Xóa sạch tất cả Subscriptions của một Channel khi Client disconnect/idle timeout.
     */
    public void remove(Channel channel) {
        // 1. Dọn dẹp Subscriptions
        topicSubscriptions.forEach((topic, subscriptions) -> {
            boolean removed = subscriptions.removeIf(sub -> sub.getChannel().id().equals(channel.id()));
            if (removed) {
                log.debug("Removed channel [{}] from topic [{}]", channel.remoteAddress(), topic);
            }
        });

        // 2. Dọn dẹp Channel Groups
        topicChannelGroups.values().forEach(group -> group.remove(channel));

        // 3. Dọn dẹp Pending ACKs và hủy Retry Task ngầm của Channel này
        String channelShortId = channel.id().asShortText();
        pendingUnAckedMessages.entrySet().removeIf(entry -> {
            if (entry.getKey().endsWith("_" + channelShortId)) {
                UnAckedMessage msg = entry.getValue();
                if (msg.getTimerTask() != null) {
                    msg.getTimerTask().cancel(false);
                }
                return true;
            }
            return false;
        });
    }

    /**
     * Broadcast tin nhắn
     */
    public void publish(String topic, byte[] payload) {
        if (clusterManager != null) {
            clusterManager.publishToCluster(topic, payload);
        } else {
            publishLocal(topic, payload);
        }
    }

    /**
     * Broadcast tin nhắn tới tất cả Subscriber trong Topic và cập nhật Metrics.
     */
    public void publishLocal(String topic, byte[] payload) {
        ChannelGroup group = topicChannelGroups.get(topic);
        if (group == null || group.isEmpty()) return;

        long msgId = idGenerator.getNextId();

        MessageResponse response = MessageResponse.newBuilder()
                .setStatus(MessageResponse.Status.SUCCESS)
                .setTopic(topic)
                .setPayload(UnsafeByteOperations.unsafeWrap(payload))
                .setMessageId(msgId)
                .build();

        for (Channel channel : group) {
            String ackKey = msgId + "_" + channel.id().asShortText();
            UnAckedMessage msg = new UnAckedMessage(msgId, channel, response);

            pendingUnAckedMessages.put(ackKey, msg);
            channel.writeAndFlush(response);

            scheduleRetry(ackKey, msg);
        }
    }

    /**
     * Lên lịch gửi lại nếu chưa nhận được ACK sau timeout
     */
    private void scheduleRetry(String ackKey, UnAckedMessage unacked) {
        ScheduledFuture<?> future = unacked.getChannel().eventLoop().schedule(() -> {
            if (!pendingUnAckedMessages.containsKey(ackKey)) return;

            if (unacked.getRetryCount() < GlobalConfig.MAX_RETRIES && unacked.getChannel().isActive()) {
                unacked.incrementRetry();
                log.warn("Retry [{}/{}] sending message [{}] to client [{}]",
                        unacked.getRetryCount(), GlobalConfig.MAX_RETRIES, unacked.getMessageId(), unacked.getChannel().remoteAddress());

                unacked.getChannel().writeAndFlush(unacked.getResponse());
                scheduleRetry(ackKey, unacked);
            } else {
                log.error("Failed to deliver message [{}] to [{}] after {} retries. Removing...",
                        unacked.getMessageId(), unacked.getChannel().remoteAddress(), GlobalConfig.MAX_RETRIES);
                pendingUnAckedMessages.remove(ackKey);
            }
        }, GlobalConfig.ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        unacked.setTimerTask(future);
    }

    /**
     * Xử lý khi nhận gói tin ACK từ Subscriber
     */
    public void acknowledge(Long messageId, Channel channel) {
        String ackKey = messageId + "_" + channel.id().asShortText();
        UnAckedMessage msg = pendingUnAckedMessages.remove(ackKey);

        if (msg != null) {
            if (msg.getTimerTask() != null) {
                // Hủy timer retry
                msg.getTimerTask().cancel(false);
            }
            log.debug("Received ACK for message [{}] from [{}]", messageId, channel.remoteAddress());
        }
    }
}
