package org.mini.pubsub.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import org.mini.pubsub.core.TopicManager;
import org.mini.pubsub.netty.handler.HeartbeatHandler;
import org.mini.pubsub.netty.handler.PubSubHandler;
import org.mini.pubsub.proto.PubSubProto;

import java.util.concurrent.TimeUnit;

public class PipelineInitializer extends ChannelInitializer<SocketChannel> {
    private static final int READ_IDLE_TIMEOUT_SECONDS = 120;
    private static final int WRITE_IDLE_TIMEOUT_SECONDS = 0;
    private static final int ALL_IDLE_TIMEOUT_SECONDS = 0;
    private final TopicManager topicManager;

    public PipelineInitializer(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("idleStateHandler", new IdleStateHandler(
                READ_IDLE_TIMEOUT_SECONDS,
                WRITE_IDLE_TIMEOUT_SECONDS,
                ALL_IDLE_TIMEOUT_SECONDS,
                TimeUnit.SECONDS
        ));
        pipeline.addLast("heartbeatHandler", new HeartbeatHandler());
        pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
        pipeline.addLast("protobufDecoder", new ProtobufDecoder(PubSubProto.MessageRequest.getDefaultInstance()));
        pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
        pipeline.addLast("protobufEncoder", new ProtobufEncoder());
        pipeline.addLast("pubSubHandler", new PubSubHandler(topicManager));
    }
}
