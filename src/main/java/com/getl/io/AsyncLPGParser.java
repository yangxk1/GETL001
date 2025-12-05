package com.getl.io;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set;

/**
 * 异步LPG解析器 - 支持并发读取多个文件并组装到TinkerPop图中
 * 针对TinkerPop的非线程安全特性，使用批量缓存+同步写入策略
 */
@Data
@Slf4j
public class AsyncLPGParser {

    // 图实例
    private Graph graph;

    // 线程池配置
    private final ExecutorService fileReaderExecutor;
    private final ExecutorService graphWriterExecutor;

    // 批量写入队列
    private final BlockingQueue<BatchData> writeQueue;

    // 并发控制
    private final ReentrantReadWriteLock graphLock = new ReentrantReadWriteLock();
    private final ConcurrentHashMap<String, Object> vertexLocks = new ConcurrentHashMap<>();
    private final CountDownLatch completionLatch;
    private final AtomicInteger activeReaders = new AtomicInteger(0);
    private final AtomicInteger activeTasks = new AtomicInteger(0);

    // 顶点缓存 - 关键性能优化
    private final ConcurrentHashMap<String, Vertex> vertexCache = new ConcurrentHashMap<>();
    private GraphTraversalSource g; // 重用GraphTraversalSource

    // 性能配置
    private static final int BATCH_SIZE = 5000; // 批量写入大小（优化：从1000增加到5000）
    private static final int QUEUE_CAPACITY = 100; // 队列容量
    private static final int DEFAULT_READER_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int WRITER_THREADS = 1; // 写入线程数（建议单线程避免竞争）

    // 数据类型常量
    public static final String INT = "int";
    public static final String INTEGER = "integer";
    public static final String LONG = "long";
    public static final String DOUBLE = "double";
    public static final String FLOAT = "float";
    public static final String DATE = "date";
    public static final String MILLI = "milli";
    public static final String STRING = "string";

    private final ThreadLocal<SimpleDateFormat> dateFormat =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));

    private Map<String, Object> defaultValue = Map.of(INT, 0, INTEGER, 0, LONG, 0L, DOUBLE, 0.0, FLOAT, 0.0f, DATE, new Date(0), MILLI, new Date(0L), STRING, "");

    // 统计信息
    private final AtomicInteger vertexCount = new AtomicInteger(0);
    private final AtomicInteger edgeCount = new AtomicInteger(0);

    /**
     * 批量数据封装
     */
    private static class BatchData {
        enum Type {VERTEX, EDGE, POISON_PILL}

        Type type;
        List<VertexData> vertices;
        List<EdgeData> edges;

        static BatchData poisonPill() {
            BatchData data = new BatchData();
            data.type = Type.POISON_PILL;
            return data;
        }

        static BatchData vertexBatch(List<VertexData> vertices) {
            BatchData data = new BatchData();
            data.type = Type.VERTEX;
            data.vertices = vertices;
            return data;
        }

        static BatchData edgeBatch(List<EdgeData> edges) {
            BatchData data = new BatchData();
            data.type = Type.EDGE;
            data.edges = edges;
            return data;
        }
    }

    @Data
    private static class VertexData {
        String id;
        String label;
        Map<String, Object> properties;
    }

    @Data
    private static class EdgeData {
        String id;
        String label;
        String fromId;
        String toId;
        String fromLabel;
        String toLabel;
        Map<String, Object> properties;
    }

    /**
     * 默认构造器
     */
    public AsyncLPGParser() {
        this(DEFAULT_READER_THREADS);
    }

    /**
     * 指定线程数的构造器
     */
    public AsyncLPGParser(int readerThreads) {
        this.graph = TinkerGraph.open();
        this.g = AnonymousTraversalSource.traversal().withEmbedded(graph); // 初始化GraphTraversalSource
        this.fileReaderExecutor = Executors.newFixedThreadPool(readerThreads,
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "LPG-Reader-" + counter.incrementAndGet());
                        t.setDaemon(true); // 设置为守护线程，防止阻止JVM退出
                        return t;
                    }
                });
        this.graphWriterExecutor = Executors.newFixedThreadPool(WRITER_THREADS,
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "LPG-Writer-" + counter.incrementAndGet());
                        t.setDaemon(true); // 设置为守护线程，防止阻止JVM退出
                        return t;
                    }
                });
        this.writeQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        this.completionLatch = new CountDownLatch(WRITER_THREADS);

        // 启动写入线程
        for (int i = 0; i < WRITER_THREADS; i++) {
            graphWriterExecutor.submit(new GraphWriter());
        }
    }

    /**
     * 使用已有图实例的构造器
     */
    public AsyncLPGParser(Graph graph, int readerThreads) {
        this(readerThreads);
        this.graph = graph;
        this.g = AnonymousTraversalSource.traversal().withEmbedded(graph); // 重新初始化
    }

    /**
     * 解析属性映射
     */
    private Map<String, String> popMap(String... pops) {
        Map<String, String> map = new HashMap<>();
        if (pops.length % 2 != 0) {
            throw new IllegalArgumentException("The number of arguments must be even, representing key-value pairs.");
        }
        for (int i = 0; i < pops.length; i += 2) {
            String key = pops[i];
            String value = pops[i + 1].toLowerCase();
            map.put(key, value);
        }
        return map;
    }

    /**
     * 解析值
     */
    private Object parseValue(String value, String type) {
        // 如果类型为空，直接返回原值
        if (StringUtils.isBlank(type)) {
            return value;
        }

        // 如果值为空，返回该类型的默认值
        if (StringUtils.isBlank(value)) {
            return defaultValue.getOrDefault(type, value);
        }

        try {
            switch (type) {
                case INT:
                case INTEGER:
                    return NumberUtils.createInteger(value);
                case LONG:
                    return NumberUtils.createLong(value);
                case DOUBLE:
                case FLOAT:
                    return NumberUtils.createDouble(value);
                case DATE:
                    try {
                        return dateFormat.get().parse(value);
                    } catch (ParseException e) {
                        return Date.valueOf(value);
                    }
                case MILLI:
                    return new Date(Long.parseLong(value));
                default:
                    return value;
            }
        } catch (Exception e) {
            log.warn("Failed to parse value: {} as type: {}, using default value for type: {}", value, type, defaultValue.getOrDefault(type, value));
            return defaultValue.getOrDefault(type, value);
        }
    }

    /**
     * 异步加载顶点文件
     */
    public CompletableFuture<Void> asyncLoadVertex(String fileName, String vertexLabel, String... pops) {
        activeTasks.incrementAndGet();
        return CompletableFuture.runAsync(() -> {
            try {
                loadVertexFile(fileName, vertexLabel, pops);
            } finally {
                activeTasks.decrementAndGet();
            }
        }, fileReaderExecutor);
    }

    /**
     * 异步加载边文件
     */
    public CompletableFuture<Void> asyncLoadEdge(String fileName, String edgeLabel,
                                                 String fromLabel, String toLabel, String... pops) {
        activeTasks.incrementAndGet();
        return CompletableFuture.runAsync(() -> {
            try {
                loadEdgeFile(fileName, edgeLabel, fromLabel, toLabel, pops);
            } finally {
                activeTasks.decrementAndGet();
            }
        }, fileReaderExecutor);
    }

    /**
     * 加载顶点文件（内部方法）
     */
    private void loadVertexFile(String fileName, String vertexLabel, String... pops) {
        Map<String, String> popMap = popMap(pops);
        activeReaders.incrementAndGet();

        try (Reader vertexReader = new FileReader(fileName)) {
            Iterable<CSVRecord> records = CSVFormat.INFORMIX_UNLOAD.withFirstRecordAsHeader().parse(vertexReader);

            List<VertexData> batch = new ArrayList<>(BATCH_SIZE);
            int count = 0;

            for (CSVRecord record : records) {
                VertexData vertexData = parseVertexRecord(record, vertexLabel, popMap);
                if (vertexData != null) {
                    batch.add(vertexData);
                    count++;

                    if (batch.size() >= BATCH_SIZE) {
                        submitBatch(BatchData.vertexBatch(new ArrayList<>(batch)));
                        batch.clear();
                    }
                }
            }

            // 提交剩余的数据
            if (!batch.isEmpty()) {
                submitBatch(BatchData.vertexBatch(batch));
            }

            log.info("Loaded {} vertices from file: {}", count, fileName);

        } catch (IOException e) {
            log.error("Failed to load vertex file: {}", fileName, e);
            throw new RuntimeException("Failed to load vertex file: " + fileName, e);
        } finally {
            activeReaders.decrementAndGet();
        }
    }

    /**
     * 加载边文件（内部方法）
     */
    private void loadEdgeFile(String fileName, String edgeLabel,
                              String fromLabel, String toLabel, String... pops) {
        Map<String, String> popMap = popMap(pops);
        activeReaders.incrementAndGet();

        try (Reader edgeReader = new FileReader(fileName)) {
            Iterable<CSVRecord> records = CSVFormat.INFORMIX_UNLOAD.withFirstRecordAsHeader().parse(edgeReader);

            List<EdgeData> batch = new ArrayList<>(BATCH_SIZE);
            int count = 0;

            for (CSVRecord record : records) {
                EdgeData edgeData = parseEdgeRecord(record, edgeLabel, fromLabel, toLabel, popMap);
                if (edgeData != null) {
                    batch.add(edgeData);
                    count++;

                    if (batch.size() >= BATCH_SIZE) {
                        submitBatch(BatchData.edgeBatch(new ArrayList<>(batch)));
                        batch.clear();
                    }
                }
            }

            // 提交剩余的数据
            if (!batch.isEmpty()) {
                submitBatch(BatchData.edgeBatch(batch));
            }

            log.info("Loaded {} edges from file: {}", count, fileName);

        } catch (IOException e) {
            log.error("Failed to load edge file: {}", fileName, e);
            throw new RuntimeException("Failed to load edge file: " + fileName, e);
        } finally {
            activeReaders.decrementAndGet();
        }
    }

    /**
     * 解析顶点记录
     */
    private VertexData parseVertexRecord(CSVRecord record, String defaultLabel, Map<String, String> popMap) {
        Map<String, String> pop = record.toMap();

        String label = pop.get("label");
        label = label == null ? defaultLabel : label;

        String id = pop.get("id");
        String idTitle = "id";
        if (StringUtils.isBlank(id)) {
            id = pop.get(label + ".id");
            idTitle = label + ".id";
        }

        if (StringUtils.isBlank(id)) {
            log.warn("Vertex record without ID, skipping");
            return null;
        }

        VertexData vertexData = new VertexData();
        vertexData.setId(label + ":" + id);
        vertexData.setLabel(label);
        vertexData.setProperties(new HashMap<>());

        // 解析属性
        for (Map.Entry<String, String> entry : pop.entrySet()) {
            if (idTitle.equals(entry.getKey()) || "id".equals(entry.getKey()) || "label".equals(entry.getKey())) {
                continue;
            }
            if (StringUtils.isNotEmpty(entry.getValue())) {
                String type = popMap.get(entry.getKey());
                Object value = parseValue(entry.getValue(), type);
                vertexData.getProperties().put(entry.getKey(), value);
            }
        }

        return vertexData;
    }

    /**
     * 解析边记录
     */
    private EdgeData parseEdgeRecord(CSVRecord record, String defaultLabel,
                                     String fromLabel, String toLabel, Map<String, String> popMap) {
        Map<String, String> pop = record.toMap();

        String label = pop.get("label");
        label = label == null ? defaultLabel : label;

        String id = pop.get("id");

        // 获取from和to的ID (通常是第一列和第二列)
        String fromId = fromLabel + ":" + record.get(0);
        String toId = toLabel + ":" + record.get(1);

        if (StringUtils.isBlank(fromId) || StringUtils.isBlank(toId)) {
            log.warn("Edge record without from/to ID, skipping");
            return null;
        }

        EdgeData edgeData = new EdgeData();
        if (StringUtils.isNotBlank(id)) {
            edgeData.setId(label + ":" + id);
        }
        edgeData.setLabel(label);
        edgeData.setFromId(fromId);
        edgeData.setToId(toId);
        edgeData.setFromLabel(fromLabel);
        edgeData.setToLabel(toLabel);
        edgeData.setProperties(new HashMap<>());

        // 解析属性
        String f1 = fromLabel + ".id";
        String t1 = toLabel + ".id";
        for (Map.Entry<String, String> entry : pop.entrySet()) {
            if (fromLabel.equals(entry.getKey()) || f1.equals(entry.getKey()) ||
                    t1.equals(entry.getKey()) || toLabel.equals(entry.getKey()) ||
                    "label".equals(entry.getKey()) || "id".equals(entry.getKey())) {
                continue;
            }
            if (StringUtils.isNotEmpty(entry.getValue())) {
                String type = popMap.get(entry.getKey());
                Object value = parseValue(entry.getValue(), type);
                edgeData.getProperties().put(entry.getKey(), value);
            }
        }

        return edgeData;
    }

    /**
     * 提交批量数据到写入队列
     */
    private void submitBatch(BatchData batchData) {
        try {
            writeQueue.put(batchData);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while submitting batch", e);
        }
    }

    /**
     * 从缓存获取或创建顶点（关键性能优化）
     */
    private Vertex getOrCreateVertex(String vertexId, String label) {
        // 先从缓存获取
        Vertex vertex = vertexCache.get(vertexId);
        if (vertex != null) {
            return vertex;
        }

        // 缓存未命中，从图中查找或创建
        Iterator<Vertex> iter = graph.vertices(vertexId);
        if (iter.hasNext()) {
            vertex = iter.next();
        } else {
            vertex = g.addV(label).property(T.id, vertexId).next();
            vertexCount.incrementAndGet();
        }

        // 放入缓存
        vertexCache.put(vertexId, vertex);
        return vertex;
    }

    /**
     * 图写入线程
     */
    private class GraphWriter implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    BatchData batchData = writeQueue.take();

                    if (batchData.type == BatchData.Type.POISON_PILL) {
                        log.info("Writer received poison pill, shutting down");
                        break;
                    }

                    if (batchData.type == BatchData.Type.VERTEX) {
                        writeVertexBatch(batchData.vertices);
                    } else if (batchData.type == BatchData.Type.EDGE) {
                        writeEdgeBatch(batchData.edges);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Writer thread interrupted", e);
            } finally {
                completionLatch.countDown();
            }
        }

        /**
         * 批量写入顶点（优化版：使用缓存）
         */
        private void writeVertexBatch(List<VertexData> vertices) {
            graphLock.writeLock().lock();
            try {
                for (VertexData vertexData : vertices) {
                    // 使用缓存获取或创建顶点
                    Vertex vertex = getOrCreateVertex(vertexData.getId(), vertexData.getLabel());

                    // 添加属性
                    for (Map.Entry<String, Object> prop : vertexData.getProperties().entrySet()) {
                        vertex.property(set, prop.getKey(), prop.getValue());
                    }
                }
            } finally {
                graphLock.writeLock().unlock();
            }
        }

        /**
         * 批量写入边（优化版：使用顶点缓存）
         */
        private void writeEdgeBatch(List<EdgeData> edges) {
            graphLock.writeLock().lock();
            try {
                for (EdgeData edgeData : edges) {
                    // 使用缓存获取或创建from顶点（关键优化：避免重复查找）
                    Vertex fromVertex = getOrCreateVertex(edgeData.getFromId(), edgeData.getFromLabel());

                    // 使用缓存获取或创建to顶点（关键优化：避免重复查找）
                    Vertex toVertex = getOrCreateVertex(edgeData.getToId(), edgeData.getToLabel());

                    // 创建边
                    GraphTraversal<Edge, Edge> addE = g.addE(edgeData.getLabel())
                            .from(fromVertex)
                            .to(toVertex);

                    if (StringUtils.isNotBlank(edgeData.getId())) {
                        addE.property(T.id, edgeData.getId());
                    }

                    // 添加属性
                    for (Map.Entry<String, Object> prop : edgeData.getProperties().entrySet()) {
                        addE.property(prop.getKey(), prop.getValue());
                    }

                    addE.next();
                    edgeCount.incrementAndGet();
                }
            } finally {
                graphLock.writeLock().unlock();
            }
        }
    }

    /**
     * 等待所有任务完成
     */
    public void awaitCompletion() throws InterruptedException {
        // 等待所有文件读取任务完成
        while (activeTasks.get() > 0 || activeReaders.get() > 0) {
            Thread.sleep(100);
        }

        log.info("All file reading tasks completed, waiting for write queue to drain");

        // 等待写入队列清空
        while (!writeQueue.isEmpty()) {
            Thread.sleep(100);
        }

        log.info("Write queue drained, sending poison pills to writers");

        // 发送终止信号
        for (int i = 0; i < WRITER_THREADS; i++) {
            writeQueue.put(BatchData.poisonPill());
        }

        // 等待写入线程完成
        completionLatch.await();

        log.info("All tasks completed. Vertices: {}, Edges: {}", vertexCount.get(), edgeCount.get());
    }

    /**
     * 关闭解析器
     */
    public void shutdown() {
        try {
            awaitCompletion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting for completion", e);
        } finally {
            fileReaderExecutor.shutdown();
            graphWriterExecutor.shutdown();
            try {
                if (!fileReaderExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    fileReaderExecutor.shutdownNow();
                }
                if (!graphWriterExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    graphWriterExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                fileReaderExecutor.shutdownNow();
                graphWriterExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 获取统计信息
     */
    public String getStatistics() {
        return String.format("Vertices: %d, Edges: %d, Cached: %d, Active Readers: %d, Queue Size: %d",
                vertexCount.get(), edgeCount.get(), vertexCache.size(), activeReaders.get(), writeQueue.size());
    }

    /**
     * 清空缓存（在需要时释放内存）
     */
    public void clearCache() {
        vertexCache.clear();
    }
}

