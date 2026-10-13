package com.example.mqtt.client;

/** MQTT 客户端异常 */
public class MqttClientException extends RuntimeException {

    public MqttClientException(String message) {
        super(message);
    }

    public MqttClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
