package com.example.mqtt.demo;

import com.example.mqtt.client.MqttEventListener;
import com.example.mqtt.client.NettyMqttClient;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 演示：订阅 test/# 并向 test/demo 分别发布 QoS0/1/2 消息。
 * 通过 {@code mqtt.client.demo.enabled=true} 开启。
 */
@Component
@ConditionalOnProperty(name = "mqtt.client.demo.enabled", havingValue = "true")
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final NettyMqttClient client;

    public DemoRunner(NettyMqttClient client) {
        this.client = client;
    }

    @Bean
    public MqttEventListener demoEventListener() {
        return new MqttEventListener() {
            @Override
            public void onConnected(boolean sessionPresent) {
                log.info("[事件] 已连接, sessionPresent={}", sessionPresent);
            }

            @Override
            public void onConnectionLost(Throwable cause) {
                log.warn("[事件] 连接丢失: {}", cause.getMessage());
            }

            @Override
            public void onReconnectScheduled(int attempt, Duration delay) {
                log.info("[事件] 第 {} 次重连, {} ms 后执行", attempt, delay.toMillis());
            }
        };
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        client.connect().get(15, TimeUnit.SECONDS);

        client.subscribe("test/#", MqttQoS.EXACTLY_ONCE, msg ->
                log.info("[收到] topic={} qos={} dup={} payload={}",
                        msg.topic(), msg.qos(), msg.dup(), msg.payloadAsString())
        ).get(5, TimeUnit.SECONDS);
        log.info("订阅 test/# 成功");

        client.publish("test/demo", "hello qos0", MqttQoS.AT_MOST_ONCE);
        client.publish("test/demo", "hello qos1", MqttQoS.AT_LEAST_ONCE)
                .get(10, TimeUnit.SECONDS);
        log.info("QoS1 发布成功（已收到 PUBACK）");
        client.publish("test/demo", "hello qos2", MqttQoS.EXACTLY_ONCE)
                .get(10, TimeUnit.SECONDS);
        log.info("QoS2 发布成功（PUBREC/PUBREL/PUBCOMP 完成）");

        // 并发压测示例：1000 条 QoS1 并行发布
        long start = System.nanoTime();
        var futures = new java.util.ArrayList<java.util.concurrent.CompletableFuture<Void>>(1000);
        for (int i = 0; i < 1000; i++) {
            futures.add(client.publish("test/bench", "msg-" + i, MqttQoS.AT_LEAST_ONCE));
        }
        java.util.concurrent.CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0]))
                .get(60, TimeUnit.SECONDS);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        log.info("1000 条 QoS1 发布完成，耗时 {} ms，吞吐 {} msg/s", elapsedMs, 1000_000L / Math.max(1, elapsedMs));
    }
}
