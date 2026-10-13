package com.example.mqtt.client.internal;

import java.util.concurrent.atomic.AtomicInteger;

/** MQTT Packet Identifier 生成器：1 ~ 65535 循环。 */
public final class PacketIdGenerator {

    private static final int MAX_PACKET_ID = 0xFFFF;

    private final AtomicInteger next = new AtomicInteger(0);

    public int nextId() {
        return next.updateAndGet(v -> v >= MAX_PACKET_ID ? 1 : v + 1);
    }
}
