package org.mini.pubsub.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.mini.pubsub.core.TopicManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServer {
    private final Logger log;
    private final int port;
    private final TopicManager topicManager;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyServer(int port, TopicManager topicManager, Logger logger) {
        this.log = logger;
        this.port = port;
        this.topicManager = topicManager;
    }

    /**
     * Khởi chạy Netty Server.
     */
    public synchronized void start() throws InterruptedException {
        boolean useEpoll = Epoll.isAvailable();
        log.info("Starting Netty Server on port [{}]. Epoll available: [{}]", port, useEpoll);

        // 1. Khởi tạo EventLoopGroups
        // Boss group: 1 thread duy nhất để lắng nghe connection mới
        // Worker group: 0 = Netty tự chọn (Số CPU cores * 2) để xử lý I/O
        if (useEpoll) {
            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup();
        } else {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
        }

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    // Chọn Channel Class phù hợp với OS (Linux Epoll vs Standard NIO)
                    .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)

                    // --- SOCKET LEVEL OPTIMIZATIONS ---
                    // Số lượng kết nối chờ trong queue của OS khi bị burst traffic
                    .option(ChannelOption.SO_BACKLOG, 4096)
                    // Cho phép reuse address ngay khi restart server
                    .option(ChannelOption.SO_REUSEADDR, true)

                    // --- CLIENT CHANNEL OPTIMIZATIONS ---
                    // Bật No delay để gửi packet ngay lập tức (giảm tối đa Latency)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    // Bật TCP KeepAlive cấp OS
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    // Sử dụng Direct Memory Pooled Allocator cho Zero-Copy
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

                    // Gắn PipelineInitializer đã định nghĩa
                    .childHandler(new PipelineInitializer(topicManager));

            // Bind Port và bắt đầu lắng nghe kết nối
            ChannelFuture future = bootstrap.bind(port).sync();
            serverChannel = future.channel();

            log.info("Netty Pub-Sub Server started successfully on port: {}", port);

            // Đăng ký Shutdown Hook để đóng server an toàn khi ngắt tiến trình Java (SIGTERM / Ctrl+C)
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        } catch (Exception e) {
            log.error("Failed to start Netty Server on port [{}]", port, e);
            stop();
            throw e;
        }
    }

    /**
     * Đóng Server an toàn (Graceful Shutdown) và giải phóng tài nguyên.
     */
    public synchronized void stop() {
        if (bossGroup == null && workerGroup == null) {
            return;
        }

        log.info("Shutting down Netty Server gracefully...");

        try {
            if (serverChannel != null) {
                serverChannel.close().sync();
            }
        } catch (InterruptedException e) {
            log.error("Error closing server channel", e);
            Thread.currentThread().interrupt();
        } finally {
            // Tắt EventLoopGroups an toàn, đợi các task dở dang hoàn tất
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }

            bossGroup = null;
            workerGroup = null;
            log.info("Netty Server stopped successfully.");
        }
    }

    /**
     * Block thread hiện tại cho đến khi Server Channel đóng hẳn.
     */
    public void awaitClose() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.closeFuture().sync();
        }
    }
}
