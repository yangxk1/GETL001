package com.getl.experiment.query;

import com.getl.converter.ConverterUtils;
import com.getl.experiment.DataLoadUtils;
import com.getl.experiment.ExperimentRunner;
import com.getl.model.MG.MGraph;
import com.getl.model.RM.RMGraph;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.eclipse.rdf4j.model.Model;

public class Q1 extends ExperimentRunner {
    public static void main(String[] args) {
        new Q1().accept();
    }

    private RMGraph graphData;

    @Override
    protected void run() {
        loadData();
        int times = 3;
        while (times-- > 0) {
            UG();
            SG();
            MG();
        }
    }

    protected void loadData() {
        this.graphData = DataLoadUtils.loadRMGraph();
    }

    protected void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGGraphFromRM(graphData);
        logger.debugInfo("transFromRM-UG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Model rdfModel = ConverterUtils.buildRDFGraphFromUG(unifiedGraph);
        logger.debugInfo("transToRM-UG", System.currentTimeMillis() - begin);

        logger.info("RDF SIZE : " + rdfModel.size());
    }

    protected void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGGraphFromRM(graphData);
        logger.debugInfo("transFromRM-MG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Model rdfModel = ConverterUtils.buildRDFGraphFromMG(mGraph);
        logger.debugInfo("transToRM-MG", System.currentTimeMillis() - begin);

        logger.info("RDF SIZE : " + rdfModel.size());
    }

    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromRM(graphData);
        logger.debugInfo("transFromRM-SG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Model rdfModel = ConverterUtils.buildRDFGraphFromSG(ogDataset);
        logger.debugInfo("transToRM-SG", System.currentTimeMillis() - begin);

        logger.info("RDF SIZE : " + rdfModel.size());
    }

    private Graph Transform(Graph graph) {
        return null;
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger("FIG14-QUERY-1");
    }
}
