package com.example.mqtt.client.internal;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * MQTT Packet Identifier 生成器：1 ~ 65535 循环。
 * @author laokou
 */
public final class PacketIdGenerator {

    private PacketIdGenerator() {}

    public static final PacketIdGenerator INSTANCE = new PacketIdGenerator();

    private final AtomicInteger next = new AtomicInteger(0);

    public int nextId() {
        return next.updateAndGet(v -> (v & 0xFFFF) + 1);
    }

}
