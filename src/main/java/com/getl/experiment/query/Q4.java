package com.getl.experiment.query;

import com.getl.converter.ConverterUtils;
import com.getl.experiment.DataLoadUtils;
import com.getl.model.MG.MGraph;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.eclipse.rdf4j.model.Model;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;

public class Q4 extends QueryRunner {
    public Q4(String name) {
        super(name);
    }

    public static void main(String[] args) {
        new Q4("UG").accept();
    }

    private Graph graphData;

    @Override
    protected void loadData() {
        this.graphData = DataLoadUtils.loadTinkerPopGraph();
    }

    @Override
    protected void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGFromTinkerPopGraph(graphData);
        logger.debugInfo("transFromLPG-UG", System.currentTimeMillis() - begin);

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
        Model resultRDF = ConverterUtils.buildRDFGraphFromUG(ugResult);
        logger.debugInfo("transTORDF-UG", System.currentTimeMillis() - begin);

        logger.info("RDF SIZE : " + resultRDF.size());
    }

    @Override
    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromTinkerPopGraph(graphData);
        logger.debugInfo("transFromLPG-SG", System.currentTimeMillis() - begin);

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
        Model resultRDF = ConverterUtils.buildRDFGraphFromSG(sgResult);
        logger.debugInfo("transTORDF-SG", System.currentTimeMillis() - begin);

        logger.info("RDF SIZE : " + resultRDF.size());

    }

    @Override
    protected void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGFromTinkerPopGraph(graphData);
        logger.debugInfo("transFromLPG-MG", System.currentTimeMillis() - begin);

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
        Model resultRDF = ConverterUtils.buildRDFGraphFromMG(mgResult);
        logger.debugInfo("transTORDF-MG", System.currentTimeMillis() - begin);

        logger.info("RDF SIZE : " + resultRDF.size());

    }

    @Override
    protected Graph Transform(Graph graph) {
        graph.traversal().V().hasLabel("Person").as("person1")
                .in("comment_hasCreator_person").as("comment")
                .out("comment_replyOf_post").as("post")
                .out("post_hasCreator_person").as("person2")
                .where("person1", P.neq("person2"))
                .not(in("person_isFanOf_person").where(eq("person1")))
                .addE("person_isFanOf_person")
                .from("person1")
                .to("person2")
                //  .select("person1", "comment", "post", "person2")
                .toList();
        //ERROR: 修改了graph值
        return graph;
    }

    @Override
    protected String loggerName() {
        return "FIG14-QUERY-4";
    }
}
