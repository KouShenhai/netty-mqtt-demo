package com.example.mqtt.client;

import io.netty.handler.codec.mqtt.MqttQoS;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * MQTT 客户端门面接口。
 *
 * <p>所有 API 均为异步非阻塞，返回 {@link CompletableFuture}：
 * <ul>
 *   <li>QoS0：发送成功后完成</li>
 *   <li>QoS1：收到 PUBACK 后完成</li>
 *   <li>QoS2：走完 PUBREC/PUBREL/PUBCOMP 完整握手后完成</li>
 * </ul>
 */
public interface MqttClient {

    /** 建立连接（若已在连接/已连接则返回同一个 future） */
    CompletableFuture<Void> connect();

    /** 主动关闭，不再自动重连 */
    CompletableFuture<Void> disconnect();

    /** 发布消息 */
    CompletableFuture<Void> publish(String topic, byte[] payload, MqttQoS qos, boolean retained);

}
