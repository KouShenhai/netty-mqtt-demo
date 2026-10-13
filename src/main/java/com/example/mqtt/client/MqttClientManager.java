package com.example.mqtt.client;

import com.example.mqtt.client.internal.InflightStore;
import com.example.mqtt.config.MqttClientProperties;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 多 MQTT 客户端实例管理器。
 *
 * <p>用于一个进程内需要连接多个 Broker（或多租户、多共享订阅组）的场景：
 * 每个命名客户端拥有独立的连接、线程池、订阅表与在途消息存储，互不影响。
 *
 * <pre>{@code
 * MqttClientProperties props = new MqttClientProperties();
 * props.setHost("emqx-1.internal");
 * manager.create("orders", props);
 * manager.start("orders");
 * manager.restart("orders");   // 手动重启（订阅与在途消息保留）
 * manager.stop("orders");      // 断开（可再 start）
 * manager.remove("orders");    // 断开并释放线程池/定时器
 * }</pre>
 */
public class MqttClientManager implements AutoCloseable {

    private final ConcurrentHashMap<String, NettyMqttClient> clients = new ConcurrentHashMap<>();

    /** 创建（或替换）一个命名客户端，不自动连接 */
    public NettyMqttClient create(String name, MqttClientProperties properties) {
        NettyMqttClient client = new NettyMqttClient(properties);
        NettyMqttClient prev = clients.put(name, client);
        if (prev != null) {
            prev.shutdown();
        }
        return client;
    }

    /** 创建并使用自定义在途消息存储（如 Redis 持久化实现） */
    public NettyMqttClient create(String name, MqttClientProperties properties, InflightStore store) {
        NettyMqttClient client = new NettyMqttClient(properties, store);
        NettyMqttClient prev = clients.put(name, client);
        if (prev != null) {
            prev.shutdown();
        }
        return client;
    }

    public NettyMqttClient get(String name) {
        NettyMqttClient client = clients.get(name);
        if (client == null) {
            throw new IllegalArgumentException("客户端不存在: " + name);
        }
        return client;
    }

    /** 启动（连接）。已关闭的客户端调用本方法即手动重启 */
    public CompletableFuture<Void> start(String name) {
        return get(name).connect();
    }

    /** 手动重启：断开并重新连接，订阅与在途消息保留 */
    public CompletableFuture<Void> restart(String name) {
        return get(name).restart();
    }

    /** 断开连接（停止自动重连），实例保留，可再次 start */
    public CompletableFuture<Void> stop(String name) {
        return get(name).disconnect();
    }

    /** 断开并彻底释放资源（IO 线程池 / 定时器），不可再重启 */
    public void remove(String name) {
        NettyMqttClient client = clients.remove(name);
        if (client != null) {
            client.shutdown();
        }
    }

    /** 全部实例的当前状态 */
    public Map<String, ClientState> status() {
        return clients.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().state()));
    }

    public Set<String> names() {
        return Set.copyOf(clients.keySet());
    }

    /** 停止并释放全部实例 */
    @Override
    public void close() {
        clients.keySet().forEach(this::remove);
    }
}
