package com.getl.experiment.RuntimeOfModelConversion;

import com.getl.converter.ConverterUtils;
import com.getl.experiment.DataLoadUtils;
import com.getl.experiment.ExperimentRunner;
import com.getl.model.MG.MGraph;
import com.getl.model.RM.RMGraph;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;

public class RM extends ExperimentRunner {

    private RMGraph rmGraph;

    public static void main(String[] args) {
        new RM().accept();
    }

    @Override
    protected void loadData() {
        this.rmGraph = DataLoadUtils.loadRMGraph();
    }

    @Override
    protected void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGGraphFromRM(rmGraph);
        logger.debugInfo("RM2UG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        RMGraph rmGraph1 = ConverterUtils.buildRMFromUGGraph(unifiedGraph, rmGraph.getSchemas());
        logger.debugInfo("UG2RM", System.currentTimeMillis() - begin);
    }

    @Override
    protected void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGGraphFromRM(rmGraph);
        logger.debugInfo("RM2MG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        RMGraph rmGraph1 = ConverterUtils.buildRMFromMG(mGraph, rmGraph.getSchemas());
        logger.debugInfo("MG2RM", System.currentTimeMillis() - begin);
    }

    @Override
    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromRM(rmGraph);
        logger.debugInfo("RM2MG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        RMGraph rmGraph1 = ConverterUtils.buildRMFromSG(ogDataset, rmGraph.getSchemas());
        logger.debugInfo("MG2RM", System.currentTimeMillis() - begin);
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger("FIG13-RM");
    }
}
