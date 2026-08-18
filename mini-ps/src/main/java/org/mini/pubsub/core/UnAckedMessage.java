package org.mini.pubsub.core;

import io.netty.channel.Channel;
import io.netty.util.concurrent.ScheduledFuture;
import org.mini.pubsub.proto.PubSubProto;

public class UnAckedMessage {
    private final Long messageId;
    private final Channel channel;
    private final PubSubProto.MessageResponse response;
    private int retryCount = 0;
    private ScheduledFuture<?> timerTask;

    public UnAckedMessage(Long messageId, Channel channel, PubSubProto.MessageResponse response) {
        this.messageId = messageId;
        this.channel = channel;
        this.response = response;
    }

    public Long getMessageId() {
        return messageId;
    }

    public Channel getChannel() {
        return channel;
    }

    public PubSubProto.MessageResponse getResponse() {
        return response;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void incrementRetry() {
        this.retryCount++;
    }

    public ScheduledFuture<?> getTimerTask() {
        return timerTask;
    }

    public void setTimerTask(ScheduledFuture<?> timerTask) {
        this.timerTask = timerTask;
    }
}
