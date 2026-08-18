package org.mini.pubsub.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeIdGenerator {
    private static final Logger log = LoggerFactory.getLogger(SnowflakeIdGenerator.class);

    private static final long START_EPOCH = 1767225600000L;

    private static final long NODE_ID_BITS = 10L;
    private static final long SEQUENCE_BITS = 12L;

    private static final long MAX_NODE_ID = ~(-1L << NODE_ID_BITS); // 1023
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS); // 4095

    private static final long NODE_ID_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + NODE_ID_BITS;

    private final long nodeId;
    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public SnowflakeIdGenerator(long nodeId) {
        if (nodeId < 0 || nodeId > MAX_NODE_ID) {
            throw new IllegalArgumentException(String.format("Node ID must be between 0 and %d", MAX_NODE_ID));
        }
        this.nodeId = nodeId;
        log.info("[SnowflakeID-Generator] Initialized with Node ID: {}", nodeId);
    }

    public synchronized long getNextId() {
        long currentTimestamp = System.currentTimeMillis();

        if (currentTimestamp < lastTimestamp) {
            // Xử lý lệch đồng hồ NTP (Clock Skew)
            long offset = lastTimestamp - currentTimestamp;
            if (offset <= 5) {
                try {
                    wait(offset << 1);
                    currentTimestamp = System.currentTimeMillis();
                    if (currentTimestamp < lastTimestamp) {
                        throw new RuntimeException("Clock moved backwards. Refusing to generate id.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException("Clock moved backwards too much. Refusing to generate id.");
            }
        }

        if (currentTimestamp == lastTimestamp) {
            // Trong cùng 1 ms, tăng sequence
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                // Đã vượt quá 4096 ID/ms, chờ sang ms tiếp theo
                currentTimestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            // Chuyển sang ms mới, reset sequence về 0
            sequence = 0L;
        }

        lastTimestamp = currentTimestamp;
        return ((currentTimestamp - START_EPOCH) << TIMESTAMP_SHIFT) | (nodeId << NODE_ID_SHIFT) | sequence;
    }

    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
