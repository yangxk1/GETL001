package com.getl.experiment.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.converter.ConverterUtils;
import com.getl.converter.RMConverter;
import com.getl.converter.TinkerPopConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.experiment.DataLoadUtils;
import com.getl.experiment.ExperimentRunner;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.MG.MGraph;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.query.step.MultiLabelP;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.eclipse.rdf4j.model.Model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;

public class Q6 extends QueryRunner {

    private Model graph;

    public static void main(String[] args) {
        new Q6().accept();
    }

    public static Set<Integer> computeComponents(Graph lpgGraph) {
        List<Vertex> vertices = lpgGraph.traversal().V().toList();
        int numVertices = vertices.size();
        List<Integer> rowOffsets = new ArrayList<>(vertices.size());
        Long count = lpgGraph.traversal().E().count().next();
        List<Integer> columnIndices = new ArrayList<>(Math.toIntExact(count));
        List<Integer> components = new ArrayList<>(numVertices);
        for (int i = 0; i < vertices.size(); i++) {
            Vertex vertex = vertices.get(i);
            vertex.property("_index", i);
            components.add(i);
        }
        int current = 0;
        for (int i = 0; i < vertices.size(); i++) {
            Vertex vertex = vertices.get(i);
            Iterator<Vertex> vertexIterator = vertex.vertices(Direction.BOTH);
            Set<Integer> neighbors = new HashSet<>();
            while (vertexIterator.hasNext()) {
                neighbors.add((Integer) vertexIterator.next().property("_index").orElse(-1));
            }
            rowOffsets.add(current);
            for (Integer neighbor : neighbors) {
                current++;
                columnIndices.add(neighbor);
            }
        }
        for (int v = 0; v < numVertices; v++) {
            compute(v, numVertices, rowOffsets, columnIndices, components);
        }
        for (int v = 0; v < numVertices; v++) {
            vertices.get(v).property("cc", components.get(v));
        }
        HashSet<Integer> set = new HashSet<>(components);
        System.out.println("cc count: " + set.size());
        return set;
    }

    public static void compute(int v, int numVertices, List<Integer> rowOffsets, List<Integer> columnIndices, List<Integer> components) {
        int start = rowOffsets.get(v);
        int end = v + 1 < numVertices ? rowOffsets.get(v + 1) : columnIndices.size();
        for (int n = start; n < end; n++) {
            int neighbor = columnIndices.get(n);
            int min = components.get(v);
            if (min < components.get(neighbor)) {
                components.set(neighbor, min);
                compute(neighbor, numVertices, rowOffsets, columnIndices, components);
            }
        }
    }

    private Map<String, Schema> buildRMSchema() {
        RMGraph rmGraph = new RMGraph();
        rmGraph.addSchema(new Schema("IRI").addColumn("origin_label", Schema.MID_TEXT).addColumn("cc", Schema.INT));
        rmGraph.addSchema(new Schema("predicate", "IRI1", "IRI2"));
        return rmGraph.getSchemas();
    }

    @Override
    protected void loadData() {
        this.graph = DataLoadUtils.loadRDFModel();
    }

    @Override
    protected void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGGraphFromRDF(graph);
        logger.debugInfo("transFromRDF-UG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph graphView = ConverterUtils.buildTinkerPopGraphFromUG(unifiedGraph);
        logger.debugInfo("graph customization-UG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph result = Transform(graphView);
        logger.debugInfo("transformation-UG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        UnifiedGraph ugResult = ConverterUtils.buildUGFromTinkerPopGraph(result);
        logger.debugInfo("ResultTransFromLPG-UG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        RMGraph resultRM = ConverterUtils.buildRMFromUGGraph(ugResult, buildRMSchema());
        logger.debugInfo("transTORM-UG", System.currentTimeMillis() - begin);
        logger.info("Lines: " + resultRM.getLines().size());

//        MysqlSessions sessions = new MysqlSessions(CommonConstant.RESULT_JDBC_URL_2, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
//        MysqlOp.createSchema(sessions);
//        MysqlOp.write(sessions, rmGraph);
    }

    @Override
    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromRDF(graph);
        logger.debugInfo("transFromRDF-SG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph graphView = ConverterUtils.buildTinkerPopGraphFromSG(ogDataset);
        logger.debugInfo("graph customization-SG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph result = Transform(graphView);
        logger.debugInfo("transformation-SG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        OGDataset sgResult = ConverterUtils.buildSGFromTinkerPopGraph(result);
        logger.debugInfo("ResultTransFromLPG-SG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        RMGraph resultRM = ConverterUtils.buildRMFromSG(sgResult, buildRMSchema());
        logger.debugInfo("transTORM-SG", System.currentTimeMillis() - begin);
        logger.info("Lines: " + resultRM.getLines().size());

    }

    @Override
    protected void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGGraphFromRDF(graph);
        logger.debugInfo("transFromRDF-MG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph graphView = ConverterUtils.buildTinkerPopGraphFromMG(mGraph);
        logger.debugInfo("graph customization-MG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph result = Transform(graphView);
        logger.debugInfo("transformation-MG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        MGraph mgResult = ConverterUtils.buildMGFromTinkerPopGraph(result);
        logger.debugInfo("ResultTransFromLPG-MG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
       RMGraph resultRM = ConverterUtils.buildRMFromMG(mgResult, buildRMSchema());
        logger.debugInfo("transTORM-MG", System.currentTimeMillis() - begin);
        logger.info("Lines: " + resultRM.getLines().size());

    }

    @Override
    protected Graph Transform(Graph graph) {
        GraphTraversalSource g = graph.traversal();// Initialize your GraphTraversalSource
// Q3: Select vertices with a degree greater than 20 and the edges between them
        //ERROR使用了原图
        List<Map<String, Object>> results = g.V()
                .property("_degree", bothE().count()) // 计算度数并存储在顶点属性中
                .has("_degree", P.gt(20)).as("n1")
                .outE().as("e1")
                .inV().has("_degree", P.gt(20)).as("n2")
                .select("n1", "e1", "n2")
                .dedup("e1") // 移除重复的边
                .toList();
        LPGGraph resultGraph = new LPGGraph();
        for (Map<String, Object> result : results) {
            Vertex n1 = (LPGVertex) result.get("n1");
            Edge e1 = (LPGEdge) result.get("e1");
            Vertex n2 = (LPGVertex) result.get("n2");
            LPGVertex n1V = resultGraph.getOrCreateVertex(n1.id(), n1.label(), n1.properties());
            LPGVertex n2V = resultGraph.getOrCreateVertex(n2.id(), n2.label(), n2.properties());
            LPGEdge lpgEdge = new LPGEdge(resultGraph, n1V, n2V, e1.label());
            lpgEdge.setId(e1.id());
        }
        return resultGraph;
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger("FIG14-QUERY-6");
    }
}