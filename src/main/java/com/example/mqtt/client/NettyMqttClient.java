package com.example.mqtt.client;

import com.example.mqtt.client.internal.InMemoryInflightStore;
import com.example.mqtt.client.internal.InflightStore;
import com.example.mqtt.client.internal.MqttProtocolHandler;
import com.example.mqtt.client.internal.OutboundMessage;
import com.example.mqtt.client.internal.PacketIdGenerator;
import com.example.mqtt.client.internal.TopicRouter;
import com.example.mqtt.config.MqttClientProperties;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 基于 Netty 的高性能 MQTT 客户端。
 *
 * <p>核心特性：
 * <ul>
 *   <li>单连接 + IO 与业务分离：报文编解码/协议状态机跑在 Netty EventLoop，
 *       消息分发跑在 JDK 虚拟线程池，互不阻塞</li>
 *   <li>指数退避 + 随机抖动的自动重连；重连后自动恢复订阅、重发在途 QoS1/2 消息（DUP=1）</li>
 *   <li>完整 QoS1（PUBACK）与 QoS2（PUBREC/PUBREL/PUBCOMP）状态机，
 *       含 ACK 超时重传与最大在途数背压</li>
 *   <li>订阅路由基于主题树，O(层级) 匹配；在途消息存储可插拔（默认内存，可换 Redis/DB）</li>
 * </ul>
 */
public class NettyMqttClient implements MqttClient {

    private static final Logger log = LoggerFactory.getLogger(NettyMqttClient.class);

    private final MqttClientProperties props;
    private final InflightStore inflightStore;
    private final TopicRouter router = new TopicRouter();
    private final PacketIdGenerator packetIdGenerator = new PacketIdGenerator();
    private final List<MqttEventListener> eventListeners = new CopyOnWriteArrayList<>();

    /** SUBACK / UNSUBACK 等待表 */
    private final Map<Integer, CompletableFuture<Integer>> pendingSubAcks = new ConcurrentHashMap<>();
    private final Map<Integer, CompletableFuture<Void>> pendingUnsubAcks = new ConcurrentHashMap<>();

    private final EventLoopGroup ioGroup;
    private final HashedWheelTimer timer;
    private final Executor dispatcher;

    private final AtomicReference<ClientState> state = new AtomicReference<>(ClientState.DISCONNECTED);
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);

    private volatile Channel channel;
    private volatile CompletableFuture<Void> connectFuture;
    private volatile Timeout reconnectTask;
    private volatile String clientId;

    public NettyMqttClient(MqttClientProperties props) {
        this(props, new InMemoryInflightStore());
    }

    public NettyMqttClient(MqttClientProperties props, InflightStore inflightStore) {
        this.props = props;
        this.inflightStore = inflightStore;
        this.ioGroup = new MultiThreadIoEventLoopGroup(
                Math.max(1, props.getIoThreads()), new DefaultThreadFactory("mqtt-io"), NioIoHandler.newFactory());
        this.timer = new HashedWheelTimer(new DefaultThreadFactory("mqtt-timer"), 100, TimeUnit.MILLISECONDS);
        // 虚拟线程分发：每条消息一个虚拟线程，业务回调可自由阻塞，不影响 IO
        this.dispatcher = Executors.newVirtualThreadPerTaskExecutor();
        this.clientId = props.getClientId() == null || props.getClientId().isBlank()
                ? "netty-mqtt-" + UUID.randomUUID().toString().substring(0, 16)
                : props.getClientId();
    }

    public void addEventListener(MqttEventListener listener) {
        eventListeners.add(listener);
    }

    // ------------------------------------------------------------------ 连接管理

    @Override
    public CompletableFuture<Void> connect() {
        // 允许显式重连：主动 disconnect() 后（CLOSED）再次调用 connect() 视为手动重启
        state.compareAndSet(ClientState.CLOSED, ClientState.DISCONNECTED);
        ClientState s = state.get();
        if (s == ClientState.CONNECTED || s == ClientState.CONNECTING) {
            return connectFuture != null ? connectFuture : CompletableFuture.completedFuture(null);
        }
        if (state.compareAndSet(s, ClientState.CONNECTING)) {
            connectFuture = new CompletableFuture<>();
            doConnect();
        }
        return connectFuture;
    }

    /**
     * 手动重启：关闭当前连接（并停止自动重连）后重新建立连接。
     *
     * <p>订阅表与在途 QoS1/2 消息全部保留，重连成功后自动恢复订阅、
     * 以 DUP=1 重发在途消息。注意：{@link #shutdown()} 释放线程池后不可再重启。
     */
    public CompletableFuture<Void> restart() {
        return disconnect().thenCompose(v -> {
            reconnectAttempts.set(0);
            return connect();
        });
    }

    private void doConnect() {
        cancelReconnectTask();
        Bootstrap bootstrap = new Bootstrap()
                .group(ioGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, props.getConnectTimeoutMs())
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        if (props.getSsl().isEnabled()) {
                            ch.pipeline().addLast("ssl", buildSslContext().newHandler(ch.alloc(),
                                    props.getHost(), props.getPort()));
                        }
                        int keepAlive = props.getKeepAliveSeconds();
                        ch.pipeline()
                                .addLast("mqtt-encoder", MqttEncoder.INSTANCE)
                                .addLast("mqtt-decoder", new MqttDecoder(props.getMaxMessageSizeBytes()))
                                // 写空闲 0.75 倍心跳发 PINGREQ；读空闲 1.5 倍判定失联
                                .addLast("idle", new IdleStateHandler(
                                        (long) (keepAlive * 1.5), (long) (keepAlive * 0.75), 0, TimeUnit.SECONDS))
                                .addLast("mqtt-handler", new MqttProtocolHandler(NettyMqttClient.this));
                    }
                });

        log.info("正在连接 MQTT Broker {}:{} (clientId={})", props.getHost(), props.getPort(), clientId);
        bootstrap.connect(props.getHost(), props.getPort()).addListener(f -> {
            if (!f.isSuccess()) {
                log.warn("TCP 连接失败: {}", f.cause().getMessage());
                onConnectFailure(f.cause());
                return;
            }
            Channel ch = (Channel) ((io.netty.channel.ChannelFuture) f).channel();
            ch.writeAndFlush(buildConnectMessage()).addListener(w -> {
                if (!w.isSuccess()) {
                    ch.close();
                    onConnectFailure(w.cause());
                }
            });
        });
    }

    private MqttConnectMessage buildConnectMessage() {
        boolean hasUser = props.getUsername() != null;
        boolean hasPassword = props.getPassword() != null;
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
                MqttVersion.MQTT_3_1_1.protocolName(),
                MqttVersion.MQTT_3_1_1.protocolLevel(),
                hasUser, hasPassword,
                false, 0, false,
                props.isCleanSession(),
                props.getKeepAliveSeconds(),
                MqttProperties.NO_PROPERTIES);
        MqttConnectPayload payload = new MqttConnectPayload(
                clientId, null, (byte[]) null,
                props.getUsername(),
                hasPassword ? props.getPassword().getBytes(java.nio.charset.StandardCharsets.UTF_8) : null);
        return new MqttConnectMessage(fixedHeader, variableHeader, payload);
    }

    private SslContext buildSslContext() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();
        if (props.getSsl().isInsecureSkipVerify()) {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        }
        return builder.build();
    }

    /** CONNACK 回调（EventLoop 线程） */
    public void onConnAck(Channel ch, boolean sessionPresent, int returnCode) {
        if (returnCode != 0) {
            log.error("连接被 Broker 拒绝, returnCode={}", returnCode);
            eventListeners.forEach(l -> safe(() -> l.onConnectionRejected(returnCode)));
            ch.close();
            onConnectFailure(new MqttClientException("CONNACK rejected, returnCode=" + returnCode));
            return;
        }
        this.channel = ch;
        reconnectAttempts.set(0);
        state.set(ClientState.CONNECTED);
        log.info("MQTT 连接成功 (sessionPresent={}, inflight={})", sessionPresent, inflightStore.size());
        CompletableFuture<Void> f = connectFuture;
        if (f != null) {
            f.complete(null);
        }
        eventListeners.forEach(l -> safe(() -> l.onConnected(sessionPresent)));

        // 1. 恢复订阅（cleanSession=true 或 Broker 无会话时必需；重复 SUBSCRIBE 对 Broker 幂等）
        restoreSubscriptions();
        // 2. 重发在途 QoS1/2 消息（DUP=1）
        retransmitInflight();
    }

    private void onConnectFailure(Throwable cause) {
        CompletableFuture<Void> f = connectFuture;
        if (f != null && !f.isDone()) {
            // 首次连接失败不直接失败 future，交给重连机制；显式 connect() 的调用方通过事件感知
            log.debug("连接失败，进入重连流程: {}", cause.getMessage());
        }
        handleDisconnect(cause);
    }

    /** 通道断开回调（EventLoop 线程） */
    public void onChannelInactive() {
        if (state.get() == ClientState.CLOSED) {
            return;
        }
        handleDisconnect(new MqttClientException("连接已断开"));
    }

    private void handleDisconnect(Throwable cause) {
        this.channel = null;
        failPendingSubRequests(new MqttClientException("连接断开", cause));
        if (state.get() == ClientState.CLOSED) {
            return;
        }
        if (!props.getReconnect().isEnabled()) {
            state.set(ClientState.DISCONNECTED);
            CompletableFuture<Void> f = connectFuture;
            if (f != null && !f.isDone()) {
                f.completeExceptionally(cause);
            }
            return;
        }
        // 只有首个把状态推进到 RECONNECTING 的调用负责排期，
        // 避免「CONNACK 拒绝 + channelInactive」等并发路径重复调度重连
        ClientState prev = state.getAndSet(ClientState.RECONNECTING);
        if (prev == ClientState.RECONNECTING) {
            return;
        }
        if (prev == ClientState.DISCONNECTED) {
            // 已放弃重连（次数耗尽）后的迟到的 channelInactive，不再重新进入重连
            state.set(ClientState.DISCONNECTED);
            return;
        }
        eventListeners.forEach(l -> safe(() -> l.onConnectionLost(cause)));
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        int maxAttempts = props.getReconnect().getMaxAttempts();
        int attempt = reconnectAttempts.incrementAndGet();
        if (maxAttempts >= 0 && attempt > maxAttempts) {
            log.error("已达最大重连次数 {}，放弃重连", maxAttempts);
            state.set(ClientState.DISCONNECTED);
            CompletableFuture<Void> f = connectFuture;
            if (f != null && !f.isDone()) {
                f.completeExceptionally(new MqttClientException("重连次数耗尽"));
            }
            return;
        }
        // 指数退避 + 抖动：delay = initial * multiplier^(n-1)，封顶 maxDelay，再叠加 ±jitter 随机
        double base = props.getReconnect().getInitialDelayMs()
                * Math.pow(props.getReconnect().getMultiplier(), attempt - 1);
        double capped = Math.min(base, props.getReconnect().getMaxDelayMs());
        double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() * 2 - 1)
                * props.getReconnect().getJitter();
        long delayMs = Math.max(100, (long) (capped * jitter));

        log.info("第 {} 次重连将在 {} ms 后进行", attempt, delayMs);
        eventListeners.forEach(l -> safe(() -> l.onReconnectScheduled(attempt, Duration.ofMillis(delayMs))));

        reconnectTask = timer.newTimeout(t -> {
            if (state.get() == ClientState.RECONNECTING && state.compareAndSet(ClientState.RECONNECTING, ClientState.CONNECTING)) {
                doConnect();
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void cancelReconnectTask() {
        Timeout task = reconnectTask;
        if (task != null) {
            task.cancel();
            reconnectTask = null;
        }
    }

    // ------------------------------------------------------------------ 发布

    @Override
    public CompletableFuture<Void> publish(String topic, byte[] payload, MqttQoS qos, boolean retained) {
        validateTopic(topic);
        if (qos == MqttQoS.AT_MOST_ONCE) {
            return publishQos0(topic, payload, retained);
        }
        return publishQos12(topic, payload, qos, retained);
    }

    private CompletableFuture<Void> publishQos0(String topic, byte[] payload, boolean retained) {
        Channel ch = channel;
        if (ch == null || state.get() != ClientState.CONNECTED) {
            return CompletableFuture.failedFuture(new MqttClientException("未连接，QoS0 消息不缓存"));
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        MqttPublishMessage msg = new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, retained, 0),
                new MqttPublishVariableHeader(topic, 0),
                Unpooled.wrappedBuffer(payload));
        ch.writeAndFlush(msg).addListener(f -> {
            if (f.isSuccess()) {
                future.complete(null);
            } else {
                future.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    private CompletableFuture<Void> publishQos12(String topic, byte[] payload, MqttQoS qos, boolean retained) {
        if (inflightStore.size() >= props.getMaxInflight()) {
            return CompletableFuture.failedFuture(
                    new MqttClientException("在途消息已达上限 " + props.getMaxInflight() + "，发布被背压拒绝"));
        }
        boolean connected = state.get() == ClientState.CONNECTED && channel != null;
        if (!connected && !props.isQueueWhenDisconnected()) {
            return CompletableFuture.failedFuture(new MqttClientException("未连接"));
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        OutboundMessage msg = null;
        // 分配未被占用的 packetId
        for (int i = 0; i < 0xFFFF; i++) {
            int id = packetIdGenerator.nextId();
            OutboundMessage candidate = new OutboundMessage(id, topic, payload, qos, retained, future);
            if (inflightStore.putIfAbsent(candidate) == null) {
                msg = candidate;
                break;
            }
        }
        if (msg == null) {
            return CompletableFuture.failedFuture(new MqttClientException("packetId 已耗尽"));
        }

        if (connected) {
            writePublish(msg);
        } else {
            log.debug("连接断开，QoS{} 消息 packetId={} 入队等待重连", qos.value(), msg.packetId());
        }
        scheduleAckTimeout(msg);
        return future;
    }

    /** 按当前协议状态写出消息（首次或 DUP 重传） */
    private void writePublish(OutboundMessage msg) {
        Channel ch = channel;
        if (ch == null || !ch.isActive()) {
            return;
        }
        msg.neverWritten = false;
        MqttPublishMessage publish = new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, msg.dup, msg.qos(), msg.retained(), 0),
                new MqttPublishVariableHeader(msg.topic(), msg.packetId()),
                Unpooled.wrappedBuffer(msg.payload()));
        ch.writeAndFlush(publish).addListener(f -> {
            if (!f.isSuccess()) {
                log.warn("PUBLISH packetId={} 写失败: {}", msg.packetId(), f.cause().getMessage());
            }
        });
    }

    private void writePubRel(int packetId) {
        Channel ch = channel;
        if (ch == null || !ch.isActive()) {
            return;
        }
        ch.writeAndFlush(new io.netty.handler.codec.mqtt.MqttMessage(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId)));
    }

    // ------------------------------------------------------------------ ACK 超时与重传

    private void scheduleAckTimeout(OutboundMessage msg) {
        msg.timeoutTask = timer.newTimeout(t -> onAckTimeout(msg), props.getAckTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private void onAckTimeout(OutboundMessage msg) {
        OutboundMessage current = inflightStore.get(msg.packetId());
        if (current != msg || msg.future().isDone()) {
            return;
        }
        if (state.get() != ClientState.CONNECTED) {
            // 连接断开期间不计重试，等重连后由 retransmitInflight 处理
            scheduleAckTimeout(msg);
            return;
        }
        if (props.getAckMaxRetries() >= 0 && msg.retries >= props.getAckMaxRetries()) {
            inflightStore.remove(msg.packetId());
            msg.future().completeExceptionally(new MqttClientException(
                    "ACK 超时重试耗尽, packetId=" + msg.packetId() + ", topic=" + msg.topic()));
            return;
        }
        msg.retries++;
        msg.dup = true;
        log.debug("ACK 超时重传 packetId={} state={} 第{}次", msg.packetId(), msg.state(), msg.retries);
        retransmit(msg);
        scheduleAckTimeout(msg);
    }

    /** 按状态机重传：等待 PUBCOMP 阶段重发 PUBREL，其余重发 PUBLISH */
    private void retransmit(OutboundMessage msg) {
        if (msg.state() == OutboundMessage.State.WAIT_PUBCOMP) {
            writePubRel(msg.packetId());
        } else {
            msg.dup = !msg.neverWritten;
            writePublish(msg);
        }
    }

    /** 重连成功后重发全部在途消息 */
    private void retransmitInflight() {
        for (OutboundMessage msg : inflightStore.all()) {
            if (msg.future().isDone()) {
                inflightStore.remove(msg.packetId());
                continue;
            }
            retransmit(msg);
            Timeout old = msg.timeoutTask;
            if (old != null) {
                old.cancel();
            }
            scheduleAckTimeout(msg);
        }
    }

    // ------------------------------------------------------------------ 出站 ACK 回调（EventLoop 线程）

    public void onPubAck(int packetId) {
        OutboundMessage msg = inflightStore.remove(packetId);
        if (msg != null) {
            cancelTimeout(msg);
            msg.future().complete(null);
        }
    }

    public void onPubRec(int packetId) {
        OutboundMessage msg = inflightStore.get(packetId);
        if (msg != null && msg.state() == OutboundMessage.State.WAIT_PUBREC) {
            msg.state = OutboundMessage.State.WAIT_PUBCOMP;
        }
    }

    public void onPubComp(int packetId) {
        OutboundMessage msg = inflightStore.remove(packetId);
        if (msg != null) {
            cancelTimeout(msg);
            msg.future().complete(null);
        }
    }

    private void cancelTimeout(OutboundMessage msg) {
        Timeout task = msg.timeoutTask;
        if (task != null) {
            task.cancel();
        }
    }

    // ------------------------------------------------------------------ 订阅

    @Override
    public CompletableFuture<Integer> subscribe(String topicFilter, MqttQoS qos, MqttMessageListener listener) {
        router.subscribe(topicFilter, qos, listener);
        Channel ch = channel;
        if (state.get() != ClientState.CONNECTED || ch == null) {
            // 本地路由已登记，连接恢复后 restoreSubscriptions 会自动补订
            return CompletableFuture.completedFuture(qos.value());
        }
        return sendSubscribe(topicFilter, qos);
    }

    private void restoreSubscriptions() {
        Map<String, MqttQoS> subs = router.activeSubscriptions();
        if (!subs.isEmpty()) {
            log.info("恢复 {} 个订阅", subs.size());
            subs.forEach(this::sendSubscribe);
        }
    }

    private CompletableFuture<Integer> sendSubscribe(String topicFilter, MqttQoS qos) {
        Channel ch = channel;
        if (ch == null) {
            return CompletableFuture.failedFuture(new MqttClientException("未连接"));
        }
        int packetId = packetIdGenerator.nextId();
        CompletableFuture<Integer> future = new CompletableFuture<>();
        pendingSubAcks.put(packetId, future);
        MqttSubscribeMessage msg = new MqttSubscribeMessage(
                new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId),
                new MqttSubscribePayload(List.of(new MqttTopicSubscription(topicFilter, qos))));
        ch.writeAndFlush(msg).addListener(f -> {
            if (!f.isSuccess()) {
                pendingSubAcks.remove(packetId);
                future.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public void onSubAck(MqttSubAckMessage msg) {
        int packetId = msg.variableHeader().messageId();
        CompletableFuture<Integer> future = pendingSubAcks.remove(packetId);
        if (future != null) {
            int granted = msg.payload().grantedQoSLevels().isEmpty()
                    ? 0x80 : msg.payload().grantedQoSLevels().get(0);
            if (granted == 0x80) {
                future.completeExceptionally(new MqttClientException("订阅被拒绝, packetId=" + packetId));
            } else {
                future.complete(granted);
            }
        }
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String topicFilter) {
        router.unsubscribe(topicFilter, null);
        Channel ch = channel;
        if (state.get() != ClientState.CONNECTED || ch == null) {
            return CompletableFuture.completedFuture(null);
        }
        int packetId = packetIdGenerator.nextId();
        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingUnsubAcks.put(packetId, future);
        MqttUnsubscribeMessage msg = new MqttUnsubscribeMessage(
                new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId),
                new MqttUnsubscribePayload(List.of(topicFilter)));
        ch.writeAndFlush(msg).addListener(f -> {
            if (!f.isSuccess()) {
                pendingUnsubAcks.remove(packetId);
                future.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    public void onUnsubAck(MqttUnsubAckMessage msg) {
        int packetId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        CompletableFuture<Void> future = pendingUnsubAcks.remove(packetId);
        if (future != null) {
            future.complete(null);
        }
    }

    private void failPendingSubRequests(Throwable cause) {
        pendingSubAcks.values().forEach(f -> f.completeExceptionally(cause));
        pendingSubAcks.clear();
        pendingUnsubAcks.values().forEach(f -> f.completeExceptionally(cause));
        pendingUnsubAcks.clear();
    }

    // ------------------------------------------------------------------ 入站消息分发

    /** 入站 PUBLISH 回调（EventLoop 线程）：拷贝载荷后移交虚拟线程分发 */
    public void onInboundMessage(MqttPublishMessage msg) {
        byte[] payload = MqttProtocolHandler.toBytes(msg.payload());
        MqttMessage message = new MqttMessage(
                msg.variableHeader().topicName(), payload,
                msg.fixedHeader().qosLevel(), msg.fixedHeader().isRetain(), msg.fixedHeader().isDup());
        List<MqttMessageListener> listeners = router.match(message.topic());
        for (MqttMessageListener listener : listeners) {
            dispatcher.execute(() -> {
                try {
                    listener.onMessage(message);
                } catch (Exception e) {
                    log.error("消息监听器处理失败 topic={}: {}", message.topic(), e.getMessage(), e);
                }
            });
        }
    }

    // ------------------------------------------------------------------ 生命周期

    @Override
    public CompletableFuture<Void> disconnect() {
        if (!state.compareAndSet(ClientState.CONNECTED, ClientState.CLOSED)
                && !state.compareAndSet(ClientState.RECONNECTING, ClientState.CLOSED)
                && !state.compareAndSet(ClientState.CONNECTING, ClientState.CLOSED)
                && !state.compareAndSet(ClientState.DISCONNECTED, ClientState.CLOSED)) {
            return CompletableFuture.completedFuture(null);
        }
        cancelReconnectTask();
        Channel ch = channel;
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (ch != null && ch.isActive()) {
            ch.writeAndFlush(new io.netty.handler.codec.mqtt.MqttMessage(
                            new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0)))
                    .addListener(f -> ch.close().addListener(c -> future.complete(null)));
        } else {
            future.complete(null);
        }
        eventListeners.forEach(l -> safe(l::onClosed));
        return future;
    }

    /** 释放全部资源（IO 线程池 / 定时器）。disconnect 之后调用。 */
    public void shutdown() {
        disconnect().join();
        timer.stop();
        ioGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS);
    }

    @Override
    public ClientState state() {
        return state.get();
    }

    @Override
    public int inflightCount() {
        return inflightStore.size();
    }

    public String clientId() {
        return clientId;
    }

    private static void validateTopic(String topic) {
        if (topic == null || topic.isBlank() || topic.contains("+") || topic.contains("#")) {
            throw new IllegalArgumentException("非法发布主题: " + topic);
        }
    }

    private static void safe(Runnable r) {
        try {
            r.run();
        } catch (Exception e) {
            log.warn("事件监听器执行异常: {}", e.getMessage());
        }
    }
}
