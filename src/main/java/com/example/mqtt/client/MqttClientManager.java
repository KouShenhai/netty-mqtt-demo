package com.example.mqtt.client;

import com.example.mqtt.client.internal.InflightStore;
import com.example.mqtt.config.MqttClientProperties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class MqttClientManager implements AutoCloseable {

    private final ConcurrentHashMap<String, NettyMqttClient> clients = new ConcurrentHashMap<>();

    /** 创建并使用自定义在途消息存储（如 Redis 持久化实现） */
    public NettyMqttClient create(String name, MqttClientProperties properties, InflightStore store) {
        NettyMqttClient client = new NettyMqttClient("laokouyun", properties, store);
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
    public CompletableFuture<Void> open(String name) {
        return get(name).connect();
    }

    /** 断开连接（停止自动重连），实例保留，可再次 start */
    public CompletableFuture<Void> close(String name) {
        return get(name).disconnect();
    }

    /** 断开并彻底释放资源（IO 线程池 / 定时器），不可再重启 */
    public void remove(String name) {
        NettyMqttClient client = clients.remove(name);
        if (client != null) {
            client.shutdown();
        }
    }

    /** 停止并释放全部实例 */
    @Override
    public void close() {
        clients.keySet().forEach(this::remove);
    }
}
