package org.mini.pubsub.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.mini.pubsub.core.TopicManager;
import org.mini.pubsub.proto.PubSubProto;
import org.mini.pubsub.proto.PubSubProto.CommandType;
import org.mini.pubsub.proto.PubSubProto.MessageRequest;
import org.mini.pubsub.proto.PubSubProto.MessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubHandler extends SimpleChannelInboundHandler<PubSubProto.MessageRequest> {
    private static final Logger log = LoggerFactory.getLogger(PubSubHandler.class);
    private final TopicManager topicManager;

    public PubSubHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    /**
     * Phương thức được gọi tự động mỗi khi Netty decode thành công một MessageRequest từ Client.
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MessageRequest request) throws Exception {
        CommandType type = request.getType();
        String topic = request.getTopic();

        if (log.isDebugEnabled()) {
            log.debug("Received command [{}] for topic [{}] from client [{}]",
                    type, topic, ctx.channel().remoteAddress());
        }

        switch (type) {
            case SUBSCRIBE:
                handleSubscribe(ctx, topic);
                break;

            case UNSUBSCRIBE:
                handleUnsubscribe(ctx, topic);
                break;

            case PUBLISH:
                handlePublish(ctx, topic, request.getPayload().toByteArray());
                break;

            case ACK:
                handleAck(ctx, request.getMessageId());
                break;

            case PING:
                handlePing(ctx);
                break;

            default:
                log.warn("Unknown command type: [{}]", type);
                sendResponse(ctx, MessageResponse.Status.ERROR, topic, "Unknown command type");
                break;
        }
    }

    private void handleSubscribe(ChannelHandlerContext ctx, String topic) {
        if (topic == null || topic.isBlank()) {
            sendResponse(ctx, MessageResponse.Status.ERROR, "",
                    "Topic cannot be empty");
            return;
        }

        topicManager.subscribe(topic, ctx.channel());
        sendResponse(ctx, MessageResponse.Status.SUCCESS, topic,
                "Subscribed successfully");
    }

    private void handleUnsubscribe(ChannelHandlerContext ctx, String topic) {
        if (topic == null || topic.isBlank()) {
            sendResponse(ctx, MessageResponse.Status.ERROR, "",
                    "Topic cannot be empty");
            return;
        }

        topicManager.unsubscribe(topic, ctx.channel());
        sendResponse(ctx, MessageResponse.Status.SUCCESS, topic,
                "Unsubscribed successfully");
    }

    private void handlePublish(ChannelHandlerContext ctx, String topic, byte[] payload) {
        if (topic == null || topic.isBlank()) {
            sendResponse(ctx, MessageResponse.Status.ERROR, "",
                    "Topic cannot be empty");
            return;
        }

        topicManager.publish(topic, payload);
    }

    private void handleAck(ChannelHandlerContext ctx, Long messageId) {
        if (messageId == null) {
            log.warn("Received invalid ACK with empty messageId from [{}]",
                    ctx.channel().remoteAddress());
            return;
        }

        topicManager.acknowledge(messageId, ctx.channel());
    }

    private void handlePing(ChannelHandlerContext ctx) {
        log.debug("Received PING from client [{}]. Timer reset", ctx.channel().remoteAddress());
    }

    private void sendResponse(ChannelHandlerContext ctx, MessageResponse.Status status,
                              String topic, String errorMsg) {
        MessageResponse.Builder builder = MessageResponse.newBuilder()
                .setStatus(status)
                .setTopic(topic != null ? topic : "");

        if (errorMsg != null) {
            builder.setErrorMessage(errorMsg);
        }

        ctx.writeAndFlush(builder.build());
    }

    /**
     * Được gọi khi Client ngắt kết nối (hoặc bị HeartbeatHandler chủ động ngắt).
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Client [{}] disconnected. Cleaning up subscriptions...", ctx.channel().remoteAddress());
        topicManager.remove(ctx.channel());
        super.channelInactive(ctx);
    }

    /**
     * Xử lý ngoại lệ không mong muốn trên Pipeline
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception caught on channel [{}]", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
