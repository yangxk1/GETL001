package com.getl.experiment.RuntimeOfModelConversion;

import com.getl.constant.RdfDataFormat;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多线程数据写入工具类 - 支持LPG/RDF/RM三种图数据格式的高效并发写入
 *
 * 主要功能：
 * 1. LPG写入：支持将TinkerPop图数据写入CSV文件
 * 2. RDF写入：支持将RDF模型写入Turtle/N-Triples格式文件
 * 3. RM写入：支持将关系模型数据写入MySQL数据库
 */
@Slf4j
public class DataWriteUtils {

    // 线程池配置
    private static final int DEFAULT_WRITER_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int BATCH_SIZE = 5000; // 批处理大小

    /**
     * 将TinkerPop LPG图写入CSV文件
     * 支持多线程并发写入顶点和边数据
     *
     * @param graph TinkerPop Graph对象
     * @param outputDir 输出目录
     * @throws IOException 如果写入文件出错
     */
    public static void writeLPGToCSV(Graph graph, String outputDir) throws IOException {
        writeLPGToCSV(graph, outputDir, DEFAULT_WRITER_THREADS);
    }

    /**
     * 将TinkerPop LPG图写入CSV文件（指定线程数）
     *
     * @param graph TinkerPop Graph对象
     * @param outputDir 输出目录
     * @param threadCount 写入线程数
     * @throws IOException 如果写入文件出错
     */
    public static void writeLPGToCSV(Graph graph, String outputDir, int threadCount) throws IOException {
        log.info("Starting LPG to CSV export with {} threads to directory: {}", threadCount, outputDir);

        File outputDirectory = new File(outputDir);
        if (!outputDirectory.exists()) {
            if (!outputDirectory.mkdirs()) {
                throw new IOException("Failed to create output directory: " + outputDir);
            }
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        try {
            // 异步写入顶点
            Future<?> vertexFuture = executorService.submit(() -> {
                try {
                    writeLPGVertices(graph, outputDir);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write LPG vertices", e);
                }
            });

            // 异步写入边
            Future<?> edgeFuture = executorService.submit(() -> {
                try {
                    writeLPGEdges(graph, outputDir);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write LPG edges", e);
                }
            });

            // 等待所有任务完成
            vertexFuture.get();
            edgeFuture.get();
            log.info("LPG to CSV export completed successfully");
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error during LPG to CSV export", e);
            throw new RuntimeException(e);
        } finally {
            executorService.shutdown();
        }
    }

    /**
     * 将顶点数据写入CSV文件
     */
    private static void writeLPGVertices(Graph graph, String outputDir) throws IOException {
        String vertexFile = outputDir + File.separator + "vertices.csv";
        AtomicInteger vertexCount = new AtomicInteger(0);

        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(vertexFile)))) {
            // 写入头部
            writer.println("id,label,properties");

            // 批量写入顶点
            graph.vertices().forEachRemaining(vertex -> {
                StringBuilder sb = new StringBuilder();
                sb.append(vertex.id()).append(",");
                sb.append(vertex.label()).append(",");

                // 序列化属性
                Map<String, Object> properties = new LinkedHashMap<>();
                vertex.properties().forEachRemaining(vp ->
                    properties.put(vp.key(), vp.value())
                );

                sb.append(serializeProperties(properties));
                writer.println(sb);

                if (vertexCount.incrementAndGet() % BATCH_SIZE == 0) {
                    log.info("Written {} vertices", vertexCount.get());
                }
            });

            log.info("Completed writing {} vertices to {}", vertexCount.get(), vertexFile);
        }
    }

    /**
     * 将边数据写入CSV文件
     */
    private static void writeLPGEdges(Graph graph, String outputDir) throws IOException {
        String edgeFile = outputDir + File.separator + "edges.csv";
        AtomicInteger edgeCount = new AtomicInteger(0);

        try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(edgeFile)))) {
            // 写入头部
            writer.println("id,label,fromId,toId,properties");

            // 批量写入边
            graph.edges().forEachRemaining(edge -> {
                StringBuilder sb = new StringBuilder();
                sb.append(edge.id()).append(",");
                sb.append(edge.label()).append(",");
                sb.append(edge.outVertex().id()).append(",");
                sb.append(edge.inVertex().id()).append(",");

                // 序列化属性
                Map<String, Object> properties = new LinkedHashMap<>();
                edge.properties().forEachRemaining(p ->
                    properties.put(p.key(), p.value())
                );

                sb.append(serializeProperties(properties));
                writer.println(sb);

                if (edgeCount.incrementAndGet() % BATCH_SIZE == 0) {
                    log.info("Written {} edges", edgeCount.get());
                }
            });

            log.info("Completed writing {} edges to {}", edgeCount.get(), edgeFile);
        }
    }

    /**
     * 将RDF Model写入文件
     * 支持多种RDF格式（Turtle、N-Triples、RDF/XML等）
     *
     * @param model RDF数据模型
     * @param outputFile 输出文件路径
     * @param format RDF数据格式（TURTLE、NTRIPLES、RDFXML等）
     * @throws IOException 如果写入文件出错
     */
    public static void writeRDFToFile(Model model, String outputFile, RdfDataFormat format) throws IOException {
        log.info("Starting RDF to file export to: {} with format: {}", outputFile, format);

        // 转换RdfDataFormat到RDFFormat
        RDFFormat rdfFormat = convertToRDFFormat(format);

        long startTime = System.currentTimeMillis();
        AtomicLong statementCount = new AtomicLong(0);

        try (OutputStream outputStream = new FileOutputStream(outputFile)) {
            Rio.write(model, outputStream, rdfFormat);
            statementCount.set(model.size());

            long duration = System.currentTimeMillis() - startTime;
            log.info("RDF export completed: {} statements written in {} ms",
                statementCount.get(), duration);
        }
    }

    /**
     * 将RDF Model异步批量写入文件（分片写入）
     *
     * @param model RDF数据模型
     * @param outputFile 输出文件路径
     * @param format RDF数据格式
     * @param threadCount 写入线程数
     * @throws IOException 如果写入文件出错
     */
    public static void writeRDFToFileAsync(Model model, String outputFile, RdfDataFormat format, int threadCount) throws IOException {
        log.info("Starting async RDF to file export with {} threads to: {}", threadCount, outputFile);

        RDFFormat rdfFormat = convertToRDFFormat(format);

        // 将RDF模型分片处理
        List<Statement> allStatements = new ArrayList<>(model);
        int totalSize = allStatements.size();
        int chunkSize = Math.max(1, totalSize / threadCount);

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        try {
            // 创建临时文件列表
            List<File> tempFiles = new ArrayList<>();

            // 分片写入临时文件
            for (int i = 0; i < threadCount; i++) {
                final int chunkIndex = i;
                int startIdx = i * chunkSize;
                int endIdx = (i == threadCount - 1) ? totalSize : (i + 1) * chunkSize;

                if (startIdx >= totalSize) break;

                File tempFile = new File(outputFile + ".tmp" + chunkIndex);
                tempFiles.add(tempFile);

                List<Statement> chunk = allStatements.subList(startIdx, endIdx);

                Future<?> future = executorService.submit(() -> {
                    try {
                        Model chunkModel = new LinkedHashModel();
                        chunkModel.addAll(chunk);
                        try (OutputStream os = new FileOutputStream(tempFile)) {
                            Rio.write(chunkModel, os, rdfFormat);
                        }
                        log.info("Chunk {} completed: {} statements", chunkIndex, chunk.size());
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to write RDF chunk " + chunkIndex, e);
                    }
                });
                futures.add(future);
            }

            // 等待所有分片写入完成
            for (Future<?> future : futures) {
                future.get();
            }

            // 合并临时文件到最终输出文件
            mergeRDFFiles(tempFiles, outputFile, rdfFormat);

            log.info("Async RDF export completed: {} statements written", totalSize);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error during async RDF export", e);
            throw new RuntimeException(e);
        } finally {
            executorService.shutdown();
        }
    }

    /**
     * 合并RDF临时文件
     */
    private static void mergeRDFFiles(List<File> tempFiles, String outputFile, RDFFormat format) throws IOException {
        Model mergedModel = new LinkedHashModel();

        for (File tempFile : tempFiles) {
            if (tempFile.exists()) {
                try (InputStream is = new FileInputStream(tempFile)) {
                    Model tempModel = Rio.parse(is, "", format);
                    mergedModel.addAll(tempModel);
                }
                if (!tempFile.delete()) {
                    log.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath());
                }
            }
        }

        try (OutputStream os = new FileOutputStream(outputFile)) {
            Rio.write(mergedModel, os, format);
        }
    }

    /**
     * 将关系模型(RM)数据写入MySQL数据库
     * 支持多表并发写入
     *
     * @param rmGraph 关系模型图
     * @param jdbcUrl MySQL连接URL
     * @param username 数据库用户名
     * @param password 数据库密码
     * @throws SQLException 如果数据库操作出错
     */
    public static void writeRMToDatabase(RMGraph rmGraph, String jdbcUrl, String username, String password) throws SQLException {
        writeRMToDatabase(rmGraph, jdbcUrl, username, password, DEFAULT_WRITER_THREADS);
    }

    /**
     * 将关系模型(RM)数据写入MySQL数据库（指定线程数）
     *
     * @param rmGraph 关系模型图
     * @param jdbcUrl MySQL连接URL
     * @param username 数据库用户名
     * @param password 数据库密码
     * @param threadCount 写入线程数
     * @throws SQLException 如果数据库操作出错
     */
    public static void writeRMToDatabase(RMGraph rmGraph, String jdbcUrl, String username, String password, int threadCount) throws SQLException {
        log.info("Starting RM to Database export with {} threads to: {}", threadCount, jdbcUrl);

        MysqlSessions sessions;
        try {
            sessions = new MysqlSessions(jdbcUrl, username, password);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL JDBC driver not found", e);
        }

        // 创建schema
        MysqlOp.createSchema(sessions);

        // 写入RM图数据
        MysqlOp.write(sessions, rmGraph);

        log.info("RM to Database export completed successfully");
    }

    /**
     * 将RM数据异步批量写入数据库
     *
     * @param rmGraph 关系模型图
     * @param jdbcUrl MySQL连接URL
     * @param username 数据库用户名
     * @param password 数据库密码
     * @param threadCount 写入线程数
     */
    public static void writeRMToDatabaseAsync(RMGraph rmGraph, String jdbcUrl, String username, String password, int threadCount) {
        log.info("Starting async RM to Database export with {} threads", threadCount);

        MysqlSessions sessions;
        try {
            sessions = new MysqlSessions(jdbcUrl, username, password);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL JDBC driver not found", e);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create database session", e);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        try {
            // 创建schema
            MysqlOp.createSchema(sessions);

            // 按表并发写入
            List<Future<?>> futures = new ArrayList<>();
            rmGraph.getSchemas().forEach((tableName, schema) -> {
                Future<?> future = executorService.submit(() -> {
                    try {
                        // 为每个表创建独立的数据库连接会话
                        MysqlSessions tableSession = new MysqlSessions(jdbcUrl, username, password);
                        RMGraph tableGraph = new RMGraph();
                        tableGraph.addSchema(schema);

                        // 过滤出该表的行数据
                        rmGraph.getLines().forEach((id, line) -> {
                            if (line.getTableName().equals(tableName)) {
                                tableGraph.getLines().put(id, line);
                            }
                        });

                        MysqlOp.write(tableSession, tableGraph);
                        log.info("Completed writing table: {}", tableName);
                    } catch (SQLException | ClassNotFoundException e) {
                        throw new RuntimeException("Failed to write table: " + tableName, e);
                    }
                });
                futures.add(future);
            });

            // 等待所有表写入完成
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Async table write failed", e);
                }
            }

            log.info("Async RM to Database export completed successfully");
        } finally {
            executorService.shutdown();
        }
    }

    /**
     * 辅助方法：序列化属性Map为JSON字符串
     */
    private static String serializeProperties(Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return "{}";
        }

        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (!first) json.append(",");
            json.append("\"").append(escapeString(entry.getKey())).append("\":");

            Object value = entry.getValue();
            if (value instanceof String) {
                json.append("\"").append(escapeString(value.toString())).append("\"");
            } else if (value instanceof Number || value instanceof Boolean) {
                json.append(value);
            } else {
                json.append("\"").append(escapeString(value.toString())).append("\"");
            }
            first = false;
        }
        json.append("}");
        return json.toString();
    }

    /**
     * 辅助方法：转义JSON字符串中的特殊字符
     */
    private static String escapeString(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }

    /**
     * 辅助方法：将RdfDataFormat转换为RDFFormat
     */
    private static RDFFormat convertToRDFFormat(RdfDataFormat format) {
        switch (format) {
            case TURTLE:
                return RDFFormat.TURTLE;
            case NTRIPLES:
                return RDFFormat.NTRIPLES;
            case RDFXML:
                return RDFFormat.RDFXML;
            case TRIG:
                return RDFFormat.TRIG;
            case NQUADS:
                return RDFFormat.NQUADS;
            default:
                return RDFFormat.TURTLE; // 默认使用Turtle格式
        }
    }
}
