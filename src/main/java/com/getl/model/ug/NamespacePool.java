package com.getl.model.ug;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class NamespacePool {
    private final Map<String, String> nsPool = new ConcurrentHashMap<>();
    private final Map<String, String> nsPoolInverse = new ConcurrentHashMap<>();
    private final AtomicInteger counter = new AtomicInteger(0);

    private NamespacePool() {}

    private static class Holder {
        private static final NamespacePool INSTANCE = new NamespacePool();
    }

    public static NamespacePool getInstance() {
        return Holder.INSTANCE;
    }

    public String getNamespaceId(String namespace) {
        if (namespace == null) namespace = "";
        return nsPool.computeIfAbsent(namespace, k -> {
            String namespaceId = String.valueOf(counter.incrementAndGet());
            nsPoolInverse.put(namespaceId, k);
            return namespaceId;
        });
    }

    public String getNamespace(String namespaceId) {
        return nsPoolInverse.get(namespaceId);
    }
}
