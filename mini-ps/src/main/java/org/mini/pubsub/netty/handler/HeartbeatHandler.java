package org.mini.pubsub.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatHandler.class);

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idleEvent) {
            if (idleEvent.state() == IdleState.READER_IDLE) {
                log.warn("Client [{}] read timeout. Closing connection...",
                        ctx.channel().remoteAddress());
                ctx.close();
            } else if (idleEvent.state() == IdleState.WRITER_IDLE) {
                log.trace("Server Writer Idle for channel [{}]", ctx.channel().remoteAddress());
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
