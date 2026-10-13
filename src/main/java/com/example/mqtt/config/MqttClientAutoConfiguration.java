package com.example.mqtt.config;

import com.example.mqtt.client.MqttClientManager;
import com.example.mqtt.client.MqttEventListener;
import com.example.mqtt.client.NettyMqttClient;
import com.example.mqtt.client.internal.InflightStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * MQTT 客户端自动装配。
 *
 * <p>扩展方式：
 * <ul>
 *   <li>注册 {@link MqttEventListener} Bean → 自动挂到客户端事件链</li>
 *   <li>注册自定义 {@link InflightStore} Bean → 替换默认内存在途存储（如 Redis 持久化）</li>
 *   <li>直接注入 {@link NettyMqttClient} 使用全部 API</li>
 * </ul>
 */
@AutoConfiguration
@EnableConfigurationProperties(MqttClientProperties.class)
public class MqttClientAutoConfiguration {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public NettyMqttClient nettyMqttClient(MqttClientProperties properties,
                                           ObjectProvider<InflightStore> inflightStore,
                                           ObjectProvider<MqttEventListener> eventListeners) {
        InflightStore store = inflightStore.getIfAvailable();
        NettyMqttClient client = store != null
                ? new NettyMqttClient(properties, store)
                : new NettyMqttClient(properties);
        eventListeners.forEach(client::addEventListener);
        if (properties.isAutoConnect()) {
            client.connect();
        }
        return client;
    }

    /** 多实例管理器：需要连接多个 Broker / 多共享订阅组 / 手动重启时使用 */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public MqttClientManager mqttClientManager() {
        return new MqttClientManager();
    }
}
