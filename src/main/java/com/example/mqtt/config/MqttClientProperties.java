package com.example.mqtt.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * MQTT 客户端配置。
 *
 * <pre>
 * mqtt:
 *   client:
 *     host: 127.0.0.1
 *     port: 1883
 *     client-id: my-client
 * </pre>
 */
@ConfigurationProperties(prefix = "mqtt.client")
public class MqttClientProperties {

    /** Broker 地址 */
    private String host = "127.0.0.1";

    /** Broker 端口 */
    private int port = 1883;

    /** 客户端 ID，留空则自动生成 */
    private String clientId = "";

    private String username;

    private String password;

    /** 是否清除会话。false 时 Broker 会保留订阅与离线 QoS1/2 消息 */
    private boolean cleanSession = true;

    /** 心跳间隔（秒） */
    private int keepAliveSeconds = 60;

    /** TCP 连接超时（毫秒） */
    private int connectTimeoutMs = 10_000;

    /** 编码允许的最大消息体字节数 */
    private int maxMessageSizeBytes = 8 * 1024 * 1024;

    /** 应用启动后是否自动连接 */
    private boolean autoConnect = true;

    /** Netty IO 线程数，0 = Netty 默认（CPU * 2） */
    private int ioThreads = 1;

    /** 最大在途（未确认）QoS1/2 消息数，超出后发布立即失败，实现背压保护 */
    private int maxInflight = 10_000;

    /** QoS1/2 单条消息 ACK 超时（毫秒） */
    private long ackTimeoutMs = 10_000;

    /** ACK 超时最大重试次数，-1 表示无限重试（消息不丢失，直到连接恢复或收到 ACK） */
    private int ackMaxRetries = 3;

    /** 连接断开时 QoS1/2 消息是否入队等待重连后发送（true=不丢消息；false=立即失败） */
    private boolean queueWhenDisconnected = true;

    private final Reconnect reconnect = new Reconnect();
    private final Ssl ssl = new Ssl();

    public static class Reconnect {
        /** 是否开启自动重连 */
        private boolean enabled = true;
        /** 首次重连延迟（毫秒） */
        private long initialDelayMs = 1_000;
        /** 重连延迟上限（毫秒） */
        private long maxDelayMs = 60_000;
        /** 退避乘数 */
        private double multiplier = 2.0;
        /** 抖动因子 0~1，避免惊群 */
        private double jitter = 0.2;
        /** 最大重连次数，-1 表示无限 */
        private int maxAttempts = -1;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public long getInitialDelayMs() { return initialDelayMs; }
        public void setInitialDelayMs(long initialDelayMs) { this.initialDelayMs = initialDelayMs; }
        public long getMaxDelayMs() { return maxDelayMs; }
        public void setMaxDelayMs(long maxDelayMs) { this.maxDelayMs = maxDelayMs; }
        public double getMultiplier() { return multiplier; }
        public void setMultiplier(double multiplier) { this.multiplier = multiplier; }
        public double getJitter() { return jitter; }
        public void setJitter(double jitter) { this.jitter = jitter; }
        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    }

    public static class Ssl {
        /** 是否启用 TLS */
        private boolean enabled = false;
        /** 跳过证书校验（仅测试环境使用） */
        private boolean insecureSkipVerify = false;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public boolean isInsecureSkipVerify() { return insecureSkipVerify; }
        public void setInsecureSkipVerify(boolean insecureSkipVerify) { this.insecureSkipVerify = insecureSkipVerify; }
    }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public boolean isCleanSession() { return cleanSession; }
    public void setCleanSession(boolean cleanSession) { this.cleanSession = cleanSession; }
    public int getKeepAliveSeconds() { return keepAliveSeconds; }
    public void setKeepAliveSeconds(int keepAliveSeconds) { this.keepAliveSeconds = keepAliveSeconds; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public void setConnectTimeoutMs(int connectTimeoutMs) { this.connectTimeoutMs = connectTimeoutMs; }
    public int getMaxMessageSizeBytes() { return maxMessageSizeBytes; }
    public void setMaxMessageSizeBytes(int maxMessageSizeBytes) { this.maxMessageSizeBytes = maxMessageSizeBytes; }
    public boolean isAutoConnect() { return autoConnect; }
    public void setAutoConnect(boolean autoConnect) { this.autoConnect = autoConnect; }
    public int getIoThreads() { return ioThreads; }
    public void setIoThreads(int ioThreads) { this.ioThreads = ioThreads; }
    public int getMaxInflight() { return maxInflight; }
    public void setMaxInflight(int maxInflight) { this.maxInflight = maxInflight; }
    public long getAckTimeoutMs() { return ackTimeoutMs; }
    public void setAckTimeoutMs(long ackTimeoutMs) { this.ackTimeoutMs = ackTimeoutMs; }
    public int getAckMaxRetries() { return ackMaxRetries; }
    public void setAckMaxRetries(int ackMaxRetries) { this.ackMaxRetries = ackMaxRetries; }
    public boolean isQueueWhenDisconnected() { return queueWhenDisconnected; }
    public void setQueueWhenDisconnected(boolean queueWhenDisconnected) { this.queueWhenDisconnected = queueWhenDisconnected; }
    public Reconnect getReconnect() { return reconnect; }
    public Ssl getSsl() { return ssl; }
}
