package org.mini.pubsub.core;

import io.netty.channel.Channel;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class Subscription {
    private final String topic;
    private final Channel channel;
    private final Instant subscribedAt;
    private final AtomicLong deliveredMessages = new AtomicLong(0);

    public Subscription(String topic, Channel channel) {
        this.topic = Objects.requireNonNull(topic, "Topic cannot be null");
        this.channel = Objects.requireNonNull(channel, "Channel cannot be null");
        this.subscribedAt = Instant.now();
    }

    public String getTopic() {
        return topic;
    }

    public Channel getChannel() {
        return channel;
    }

    public Instant getSubscribedAt() {
        return subscribedAt;
    }

    public long getDeliveredMessages() {
        return deliveredMessages.get();
    }

    public void incrementDeliveredMessages() {
        this.deliveredMessages.incrementAndGet();
    }

    /**
     * Kiểm tra Channel còn Active hay không trước khi gửi dữ liệu.
     */
    public boolean isActive() {
        return channel.isActive();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subscription that = (Subscription) o;
        return Objects.equals(topic, that.topic) && Objects.equals(channel.id(), that.channel.id());
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, channel.id());
    }

    @Override
    public String toString() {
        return "Subscription{" +
                "topic='" + topic + '\'' +
                ", channelId=" + channel.id().asShortText() +
                ", remoteAddress=" + channel.remoteAddress() +
                ", subscribedAt=" + subscribedAt +
                ", delivered=" + deliveredMessages.get() +
                '}';
    }
}
