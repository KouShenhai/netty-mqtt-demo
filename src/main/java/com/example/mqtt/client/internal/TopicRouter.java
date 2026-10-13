package com.example.mqtt.client.internal;

import com.example.mqtt.client.MqttMessageListener;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于主题树（Trie）的订阅路由。
 *
 * <p>支持 {@code +}（单级）与 {@code #}（多级）通配符；
 * 查找复杂度为 O(主题层级数)，与订阅数量无关，适合大量订阅的高频分发场景。
 * 读（分发）走读锁，写（增删订阅）走写锁，订阅不频繁时几乎无锁竞争。
 *
 * <p>支持 EMQX 共享订阅：{@code $share/<group>/<filter>} 与 {@code $queue/<filter>}。
 * Broker 投递共享订阅消息时使用真实 topic（不含前缀），因此路由树按剥离前缀后的
 * 过滤器建索引做本地匹配；而重连恢复订阅时仍使用原始过滤器发给 Broker。
 */
public class TopicRouter {

    /** 一条订阅记录 */
    public record Subscription(String topicFilter, MqttQoS qos, MqttMessageListener listener) {}

    private static final class Node {
        final Map<String, Node> children = new HashMap<>();
        final List<Subscription> subscriptions = new ArrayList<>();
    }

    private final Node root = new Node();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** 全量订阅（用于重连后恢复订阅，key 为发给 Broker 的原始过滤器） */
    private final Map<String, MqttQoS> subscriptions = new LinkedHashMap<>();

    /**
     * 将过滤器拆成用于本地匹配的层级数组。
     * $share/group/a/b → [a, b]；$queue/a/b → [a, b]；普通过滤器原样拆分。
     */
    static String[] matchLevels(String topicFilter) {
        String[] levels = topicFilter.split("/");
        if (levels.length >= 3 && "$share".equals(levels[0])) {
            return java.util.Arrays.copyOfRange(levels, 2, levels.length);
        }
        if (levels.length >= 2 && "$queue".equals(levels[0])) {
            return java.util.Arrays.copyOfRange(levels, 1, levels.length);
        }
        return levels;
    }

    public void subscribe(String topicFilter, MqttQoS qos, MqttMessageListener listener) {
        lock.writeLock().lock();
        try {
            Node node = root;
            for (String level : matchLevels(topicFilter)) {
                node = node.children.computeIfAbsent(level, k -> new Node());
            }
            Subscription sub = new Subscription(topicFilter, qos, listener);
            if (!node.subscriptions.contains(sub)) {
                node.subscriptions.add(sub);
            }
            subscriptions.merge(topicFilter, qos, (a, b) -> a.value() >= b.value() ? a : b);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unsubscribe(String topicFilter, MqttMessageListener listener) {
        lock.writeLock().lock();
        try {
            remove(root, matchLevels(topicFilter), 0, listener);
            // 该过滤器下已无监听器时才从恢复表中移除
            if (collect(topicFilter).isEmpty()) {
                subscriptions.remove(topicFilter);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean remove(Node node, String[] levels, int idx, MqttMessageListener listener) {
        if (idx == levels.length) {
            node.subscriptions.removeIf(s -> listener == null || s.listener() == listener);
            return node.subscriptions.isEmpty();
        }
        Node child = node.children.get(levels[idx]);
        if (child != null && remove(child, levels, idx + 1, listener) && child.children.isEmpty()) {
            node.children.remove(levels[idx]);
        }
        return node.subscriptions.isEmpty();
    }

    /** 查找匹配某主题的所有监听器 */
    public List<MqttMessageListener> match(String topic) {
        lock.readLock().lock();
        try {
            List<MqttMessageListener> result = new ArrayList<>();
            match(root, topic.split("/"), 0, result);
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void match(Node node, String[] levels, int idx, List<MqttMessageListener> result) {
        // 多级通配符 # 匹配剩余所有层级
        Node hash = node.children.get("#");
        if (hash != null) {
            hash.subscriptions.forEach(s -> result.add(s.listener()));
        }
        if (idx == levels.length) {
            node.subscriptions.forEach(s -> result.add(s.listener()));
            return;
        }
        Node exact = node.children.get(levels[idx]);
        if (exact != null) {
            match(exact, levels, idx + 1, result);
        }
        Node plus = node.children.get("+");
        if (plus != null) {
            match(plus, levels, idx + 1, result);
        }
    }

    /** 重连后需要恢复的全部订阅：topicFilter -> qos */
    public Map<String, MqttQoS> activeSubscriptions() {
        lock.readLock().lock();
        try {
            return Map.copyOf(subscriptions);
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<Subscription> collect(String topicFilter) {
        Node node = root;
        for (String level : matchLevels(topicFilter)) {
            node = node.children.get(level);
            if (node == null) {
                return List.of();
            }
        }
        return node.subscriptions;
    }

    public int subscriptionCount() {
        lock.readLock().lock();
        try {
            return subscriptions.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
