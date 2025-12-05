package com.getl.experiment.RuntimeOfModelConversion;

import com.getl.converter.ConverterUtils;
import com.getl.experiment.DataLoadUtils;
import com.getl.experiment.ExperimentRunner;
import com.getl.model.MG.MGraph;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;

import java.util.Map;

public class RM extends ExperimentRunner {

    private RMGraph rmGraph;

    private Map<String, Schema> schemaMap;

    public RM(String name) {
        super(name);
    }

    public static void main(String[] args) {
        new RM("UG").accept();
    }

    @Override
    protected void loadData() {
        this.rmGraph = DataLoadUtils.loadRMGraph();
        this.schemaMap = rmGraph.getSchemas();
    }

    @Override
    protected void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGGraphFromRM(rmGraph);
        logger.debugInfo("RM2UG", System.currentTimeMillis() - begin);
        rmGraph = null;
        super.forceGC();
        begin = System.currentTimeMillis();
        RMGraph rmGraph1 = ConverterUtils.buildRMFromUGGraph(unifiedGraph, this.schemaMap);
        logger.debugInfo("UG2RM", System.currentTimeMillis() - begin);
    }

    @Override
    protected void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGGraphFromRM(rmGraph);
        logger.debugInfo("RM2MG", System.currentTimeMillis() - begin);
        rmGraph = null;
        super.forceGC();
        begin = System.currentTimeMillis();
        RMGraph rmGraph1 = ConverterUtils.buildRMFromMG(mGraph, this.schemaMap);
        logger.debugInfo("MG2RM", System.currentTimeMillis() - begin);
    }

    @Override
    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromRM(rmGraph);
        logger.debugInfo("RM2MG", System.currentTimeMillis() - begin);
        rmGraph = null;
        super.forceGC();
        begin = System.currentTimeMillis();
        RMGraph rmGraph1 = ConverterUtils.buildRMFromSG(ogDataset, this.schemaMap);
        logger.debugInfo("MG2RM", System.currentTimeMillis() - begin);
    }

    @Override
    protected String loggerName() {
        return "FIG13-RM";
    }
}
