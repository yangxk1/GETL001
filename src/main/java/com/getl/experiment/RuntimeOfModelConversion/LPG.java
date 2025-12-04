package com.getl.experiment.RuntimeOfModelConversion;

import com.getl.constant.CommonConstant;
import com.getl.converter.ConverterUtils;
import com.getl.example.Runnable;
import com.getl.experiment.DataLoadUtils;
import com.getl.experiment.ExperimentRunner;
import com.getl.io.AsyncLPGParser;
import com.getl.model.MG.MGraph;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.CompletableFuture;

public class LPG extends ExperimentRunner {
    public LPG(String name) {
        super(name);
    }

    public static void main(String[] args) {
        new LPG("UG").accept();
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
        logger.debugInfo("TinkerPopGraph2UG", System.currentTimeMillis() - begin);
        graphData = null;
        super.forceGC();
        begin = System.currentTimeMillis();
        Graph graph = ConverterUtils.buildTinkerPopGraphFromUG(unifiedGraph);
        logger.debugInfo("UG2TinkerPopGraph", System.currentTimeMillis() - begin);
    }

    @Override
    protected void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGFromTinkerPopGraph(graphData);
        logger.debugInfo("TinkerPopGraph2MG", System.currentTimeMillis() - begin);
        graphData = null;
        super.forceGC();
        begin = System.currentTimeMillis();
        Graph graph = ConverterUtils.buildTinkerPopGraphFromMG(mGraph);
        logger.debugInfo("MG2TinkerPopGraph", System.currentTimeMillis() - begin);
    }

    @Override
    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromTinkerPopGraph(graphData);
        logger.debugInfo("TinkerPopGraph2SG", System.currentTimeMillis() - begin);
        graphData = null;
        super.forceGC();
        begin = System.currentTimeMillis();
        Graph graph = ConverterUtils.buildTinkerPopGraphFromSG(ogDataset);
        logger.debugInfo("SG2TinkerPopGraph", System.currentTimeMillis() - begin);
    }

    @Override
    protected String loggerName() {
        return "FIG13-LPG";
    }
}
