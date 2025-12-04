package com.getl.experiment.query;

import com.getl.converter.ConverterUtils;
import com.getl.experiment.DataLoadUtils;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.MG.MGraph;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.Model;

import java.util.List;
import java.util.Map;

public class Q3 extends QueryRunner {

    private Model graphData;

    public Q3(String name) {
        super(name);
    }

    public static void main(String[] args) {
        new Q3("UG").accept();
    }

    @Override
    protected void loadData() {
        this.graphData = DataLoadUtils.loadRDFModel();
    }

    @Override
    protected void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGGraphFromRDF(graphData);
        logger.debugInfo("transFromRDF-UG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        graphData = null;
        super.forceGC();
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
        Graph resultGraph = ConverterUtils.buildTinkerPopGraphFromUG(ugResult);
        logger.debugInfo("transTOLPG-UG", System.currentTimeMillis() - begin);

        logger.info("lpg vertex count: " + resultGraph.traversal().V().count().next());
        logger.info("lpg edge count: " + resultGraph.traversal().E().count().next());
    }

    @Override
    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromRDF(graphData);
        logger.debugInfo("transFromRDF-SG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        graphData = null;
        super.forceGC();
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
        Graph resultGraph = ConverterUtils.buildTinkerPopGraphFromSG(sgResult);
        logger.debugInfo("transTOLPG-SG", System.currentTimeMillis() - begin);

        logger.info("lpg vertex count: " + resultGraph.traversal().V().count().next());
        logger.info("lpg edge count: " + resultGraph.traversal().E().count().next());

    }

    @Override
    protected void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGGraphFromRDF(graphData);
        logger.debugInfo("transFromRDF-MG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        graphData = null;
        super.forceGC();
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
        Graph resultGraph = ConverterUtils.buildTinkerPopGraphFromMG(mgResult);
        logger.debugInfo("transTOLPG-MG", System.currentTimeMillis() - begin);

        logger.info("lpg vertex count: " + resultGraph.traversal().V().count().next());
        logger.info("lpg edge count: " + resultGraph.traversal().E().count().next());

    }

    @Override
    protected Graph Transform(Graph graph) {
        GraphTraversalSource g = graph.traversal();// Initialize your GraphTraversalSource
// Q3: Select vertices with a degree greater than 5 and the edges between them
        g.V()
                .property("_degree", __.bothE().count()).toList();
        List<Map<String, Object>> results = graph.traversal().V()
                .has("_degree", P.gt(20)).as("n1")
                .outE().as("e1")
                .inV().has("_degree", P.gt(20)).as("n2")
                .select("n1", "e1", "n2")
                .dedup("e1")
                .toList();
        //  System.out.println(results.size());
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
    protected String loggerName() {
        return "FIG14-QUERY-3";
    }
}
