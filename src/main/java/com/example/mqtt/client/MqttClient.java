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

    default CompletableFuture<Void> publish(String topic, byte[] payload, MqttQoS qos) {
        return publish(topic, payload, qos, false);
    }

    default CompletableFuture<Void> publish(String topic, String payload, MqttQoS qos) {
        return publish(topic, payload.getBytes(StandardCharsets.UTF_8), qos, false);
    }

    /**
     * 订阅主题（支持 + 与 # 通配符）。
     *
     * @return future 完成值为 Broker 授予的 QoS 等级
     */
    CompletableFuture<Integer> subscribe(String topicFilter, MqttQoS qos, MqttMessageListener listener);

    /** 取消订阅 */
    CompletableFuture<Void> unsubscribe(String topicFilter);

    ClientState state();

    default boolean isConnected() {
        return state() == ClientState.CONNECTED;
    }

    /** 当前在途（未确认）QoS1/2 消息数 */
    int inflightCount();
}
