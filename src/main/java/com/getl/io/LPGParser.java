package com.getl.io;

import com.getl.converter.TinkerPopConverter;
import com.getl.converter.async.AsyncPG2UMG;
import com.getl.model.ug.UnifiedGraph;
import lombok.Data;
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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set;
import static org.javacc.parser.JavaCCGlobals.fileName;

@Data
public class LPGParser {
    private Graph graph;
    private AsyncPG2UMG asyncPG2UMG;

    private CountDownLatch latch;

    public void latchSize(int size) {
        latch = new CountDownLatch(size);
    }

    public void waitAll() throws InterruptedException {
        latch.await();
    }

    public LPGParser() {
        graph = TinkerGraph.open();
    }

    public LPGParser(TinkerPopConverter tinkerPopConverter) {
        this();
        asyncPG2UMG = new AsyncPG2UMG(tinkerPopConverter);
    }

    public LPGParser(Graph graph) {
        this.graph = graph;
    }

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

    public static final String INT = "int";
    public static final String INTEGER = "integer";
    public static final String LONG = "long";
    public static final String DOUBLE = "double";
    public static final String FLOAT = "float";
    public static final String DATE = "date";
    public static final String MILLI = "milli";
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private Object parseValue(String value, String type) {
        if (StringUtils.isBlank(type)) {
            return value;
        }
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
                    return dateFormat.parse(value);
                } catch (ParseException e) {
                    return Date.valueOf(value);
                }
//                return Date.valueOf(value);
            case MILLI:
                return new Date(Long.parseLong(value));
            default:
                return value;
        }
    }

    public void asyncLoadVertex(String fileName, String vertexLabel, String... pops) {
        new Thread(() -> {
            loadVertex(fileName, vertexLabel, pops);
        }).start();
    }

    private List<Element> elementCache = new ArrayList<>(2048);

    public LPGParser loadVertex(String fileName, String vertexLabel, String... pops) {
        System.out.println("READING " + fileName);
        System.out.println(System.currentTimeMillis());
        Map<String, String> popMap = popMap(pops);
        try (Reader vertexReader = new FileReader(fileName)) {
            Iterable<CSVRecord> records = CSVFormat.INFORMIX_UNLOAD.withFirstRecordAsHeader().parse(vertexReader);
            loadVertex(records, vertexLabel, popMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public void loadVertex(Iterable<CSVRecord> records, String vertexLabel, Map<String, String> popMap) throws IOException {
        int i = 0;
        for (CSVRecord record : records) {
//            if (i++ >= 3000){break;}
            Map<String, String> pop = record.toMap();
            String label = pop.get("label");
            label = label == null ? vertexLabel : label;
            String id = pop.get("id");
            String idTitle = "id";
            if (StringUtils.isBlank(id)) {
                id = pop.get(label + ".id");
                idTitle = label + ".id";
            }
            Vertex vertex;
            Iterator<Vertex> vertices = graph.vertices(label + ":" + id);
            if (StringUtils.isNotBlank(id) && vertices.hasNext()) {
                vertex = vertices.next();
            } else {
                GraphTraversalSource g = AnonymousTraversalSource.traversal().withEmbedded(graph);
                GraphTraversal<Vertex, Vertex> addV = g.addV(label);
                if (StringUtils.isNotBlank(id)) {
                    addV.property(T.id, label + ":" + id);
                }
                vertex = addV.next();
                elementCache.add(vertex);
            }
            for (Map.Entry<String, String> entry : pop.entrySet()) {
                if (idTitle.equals(entry.getKey()) || "id".equals(entry.getKey())) {
                    continue;
                }
                if (StringUtils.isNotEmpty(entry.getValue())) {
                    String type = popMap.get(entry.getKey());
                    vertex.property(set, entry.getKey(), parseValue(entry.getValue(), type));
                }
            }
            //if (asyncPG2UMG != null) {

         //   }
        }
    }

    public void commit2Converter() {
        List<Element> cache = elementCache;
        this.elementCache = new ArrayList<>(2048);
        new Thread(() -> {
            cache.forEach(asyncPG2UMG::addElement);
            latch.countDown();
        }).start();
    }

    public void asyncLoadEdge(String fileName, String edgeLabel, String from, String to, String... pops) {
        new Thread(() -> {
            loadEdge(fileName, edgeLabel, from, to, pops);
        }).start();
    }

    public LPGParser loadEdge(String fileName, String edgeLabel, String from, String to, String... pops) {
        System.out.println("READING " + fileName);
        System.out.println(System.currentTimeMillis());
        Map<String, String> popMap = popMap(pops);
        try (Reader edgeReader = new FileReader(fileName)) {
            Iterable<CSVRecord> records = CSVFormat.INFORMIX_UNLOAD.withFirstRecordAsHeader().parse(edgeReader);
            loadEdge(records, edgeLabel, from, to, popMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public void loadEdge(Iterable<CSVRecord> records, String edgeLabel, String from, String to, Map<String, String> popMap) throws IOException {
        String fromLabel = from == null ? "from" : from;
        String toLabel = to == null ? "to" : to;
        from = fromLabel + ".id";
        to = toLabel + ".id";
        GraphTraversalSource g = AnonymousTraversalSource.traversal().withEmbedded(graph);
        for (CSVRecord record : records) {
//            if (i++ >= 3000){break;}
            Map<String, String> pop = record.toMap();
            String label = pop.get("label");
            label = label == null ? edgeLabel : label;
            String id = pop.get("id");
            GraphTraversal<Edge, Edge> addE = g.addE(label);
            Vertex fromVertex, toVertex;
            String fromId = fromLabel + ":" + pop.get(from);
            String toId = toLabel + ":" + pop.get(to);
            if (graph.vertices(fromId).hasNext()) {
                fromVertex = graph.vertices(fromId).next();
            } else {
                fromVertex = AnonymousTraversalSource.traversal().withEmbedded(graph).addV(fromLabel).property(T.id, fromId).next();
            }
            addE.from(fromVertex);
            if (graph.vertices(toId).hasNext()) {
                toVertex = graph.vertices(toId).next();
            } else {
                toVertex = AnonymousTraversalSource.traversal().withEmbedded(graph).addV(toLabel).property(T.id, toId).next();
            }
            addE.to(toVertex);
            if (StringUtils.isNotBlank(id)) {
                addE.property(T.id, label + ":" + id);
            }
            for (Map.Entry<String, String> entry : pop.entrySet()) {
                if (from.equals(entry.getKey()) || to.equals(entry.getKey()) || "label".equals(entry.getKey()) || "id".equals(entry.getKey())) {
                    continue;
                }
                if (StringUtils.isNotEmpty(entry.getValue())) {
                    String type = popMap.get(entry.getKey());
                    addE.property(entry.getKey(), parseValue(entry.getValue(), type));
                }
            }
            Edge next = addE.next();
          //  if (asyncPG2UMG != null) {
                elementCache.add(next);
          //  }
        }
    }
}
