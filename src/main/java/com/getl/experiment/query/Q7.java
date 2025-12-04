package com.getl.experiment.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.converter.ConverterUtils;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.RMConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LDBC2UGUtil;
import com.getl.example.utils.RandomWalk;
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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.Model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Q7 extends QueryRunner {
    public static void main(String[] args) {
        new Q7().accept();
    }

    private Graph tinkerPopGraph;
    private Model rdfModel;
    private RMGraph rmGraph;

    @Override
    protected void run() {
        try {
            System.out.println("BEGIN TO TEST Q7 time: " + System.currentTimeMillis());
            UnifiedGraph unifiedGraph = new UnifiedGraph();
            long begin = System.currentTimeMillis();
            LDBC2UGUtil.loadFromRM(unifiedGraph, logger);
            Runtime.getRuntime().gc();
            LDBC2UGUtil.loadFromRDF(unifiedGraph, logger);
            Runtime.getRuntime().gc();
            LDBC2UGUtil.loadFromPG(unifiedGraph, logger);
            Runtime.getRuntime().gc();

            Runtime.getRuntime().gc();
            logger.debugInfo("GC", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            RMConverter rmConverter = new RMConverter(unifiedGraph, rmGraph);
            rmConverter.addUGMToRMModel();
            logger.debugInfo("ugm 2 RM end ", (System.currentTimeMillis() - begin));
            System.out.println("Lines: " + rmGraph.getLines().size());
            begin = System.currentTimeMillis();
            MysqlSessions sessions = new MysqlSessions(CommonConstant.LDBC_JDBC_RESULT, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
            MysqlOp.createSchema(sessions);
            MysqlOp.write(sessions, rmGraph);
            logger.debugInfo("Write RM end " + (System.currentTimeMillis() - begin));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void loadData() {
        new Thread(() -> {
            tinkerPopGraph = DataLoadUtils.loadTinkerPopGraph();
        }).start();
        new Thread(() -> {
            rdfModel = DataLoadUtils.loadRDFModel();
        }).start();
        new Thread(() -> {
            rmGraph = DataLoadUtils.loadRMGraph();
        }).start();
    }

    @Override
    protected void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = new UnifiedGraph();
        ConverterUtils.buildUGFromTinkerPopGraph(tinkerPopGraph, unifiedGraph);
        ConverterUtils.buildUGGraphFromRDF(rdfModel, unifiedGraph);
        ConverterUtils.buildUGGraphFromRM(rmGraph, unifiedGraph);
        logger.debugInfo("transFromLPG_RDF_RM-UG", System.currentTimeMillis() - begin);

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
    }

    @Override
    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = new OGDataset();
        ConverterUtils.buildSGFromTinkerPopGraph(tinkerPopGraph, ogDataset);
        ConverterUtils.buildSGFromRDF(rdfModel, ogDataset);
        ConverterUtils.buildSGFromRM(rmGraph, ogDataset);
        logger.debugInfo("transFromLPG_RDF_RM-SG", System.currentTimeMillis() - begin);

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
        MGraph mGraph = new MGraph();
        ConverterUtils.buildMGFromTinkerPopGraph(tinkerPopGraph, mGraph);
        ConverterUtils.buildMGGraphFromRDF(rdfModel, mGraph);
        ConverterUtils.buildMGGraphFromRM(rmGraph, mGraph);
        logger.debugInfo("transFromLPG_RDF_RM-MG", System.currentTimeMillis() - begin);

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
        RandomWalk randomWalk = new RandomWalk(graph);
        LPGGraph resultGraph = new LPGGraph();
        List<List<Vertex>> lists = randomWalk.forward(3);
        for (int i = 0; i < lists.size(); i++) {
            List<Vertex> vertexIds = lists.get(i);
            if (vertexIds.size() < 3) {
                continue;
            }
            Vertex v1 = vertexIds.get(0);
            Vertex e = vertexIds.get(1);
            Vertex v2 = vertexIds.get(2);
            LPGVertex n1V = resultGraph.getOrCreateVertex(v1.id(), v1.label(), v1.properties());
            LPGVertex n2V = resultGraph.getOrCreateVertex(v2.id(), v2.label(), v2.properties());
            LPGEdge lpgEdge = new LPGEdge(resultGraph, n1V, n2V, "recommend");
            lpgEdge.addPropertyValue("post", e.id());
        }
        System.out.println("vertices count: " + resultGraph.getVertices().size());
        System.out.println("edges count: " + resultGraph.getEdges().size());
        return resultGraph;
    }

    private Map<String, Schema> buildRMSchema() {
        RMGraph rmGraph = new RMGraph();
        rmGraph.addSchema(new Schema("recommend", "Person1", "Person", "Person2", "Person").addColumn("post", Schema.SMALL_TEXT));
        rmGraph.addSchema(new Schema("Person").addColumn("birthday", Schema.DATE)
                .addColumn("firstName", Schema.SMALL_TEXT).addColumn("lastName", Schema.SMALL_TEXT).
                addColumn("gender", Schema.SMALL_TEXT).addColumn("browserUsed", Schema.SMALL_TEXT).
                addColumn("locationIP", Schema.SMALL_TEXT).addColumn("language", Schema.SMALL_TEXT).
                addColumn("creationDate", Schema.DATE).addColumn("email", Schema.MID_LARGE_TEXT));
        return rmGraph.getSchemas();
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger("QUERY 7");
    }
}