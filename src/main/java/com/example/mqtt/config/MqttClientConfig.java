package com.example.mqtt.config;

import com.example.mqtt.client.MqttClientManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MqttClientConfig {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public MqttClientManager mqttClientManager() {
        return new MqttClientManager();
    }
}
