package com.getl.experiment.RuntimeOfModelConversion;

import com.getl.constant.CommonConstant;
import com.getl.converter.ConverterUtils;
import com.getl.experiment.DataLoadUtils;
import com.getl.experiment.ExperimentRunner;
import com.getl.model.MG.MGraph;
import com.getl.model.RM.RMGraph;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;

public class RM2LPG extends ExperimentRunner {
    public RM2LPG(String name) {
        super(name);
    }

    public static void main(String[] args) {
        new RM2LPG("UG").accept();
    }

    private RMGraph graphData;

    @Override
    protected void loadData() {
        this.graphData = DataLoadUtils.loadRMGraph();
    }

    @Override
    protected void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGGraphFromRM(graphData);
        logger.debugInfo("RM2UG", System.currentTimeMillis() - begin);
        graphData = null;
        super.forceGC();
        begin = System.currentTimeMillis();
        Graph graph = ConverterUtils.buildTinkerPopGraphFromUG(unifiedGraph);
        logger.debugInfo("UG2LPG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        try {
            DataWriteUtils.writeLPGToCSV(graph, CommonConstant.RM_TO_LPG_RESULT_BASE_URL + "ug.csv");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.debugInfo("WRITE TO CSV", System.currentTimeMillis() - begin);

    }

    @Override
    protected void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGGraphFromRM(graphData);
        logger.debugInfo("RM2MG", System.currentTimeMillis() - begin);
        graphData = null;
        super.forceGC();
        begin = System.currentTimeMillis();
        Graph graph = ConverterUtils.buildTinkerPopGraphFromMG(mGraph);
        logger.debugInfo("MG2LPG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        try {
            DataWriteUtils.writeLPGToCSV(graph, CommonConstant.RM_TO_LPG_RESULT_BASE_URL + "mg.csv");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.debugInfo("WRITE TO CSV", System.currentTimeMillis() - begin);
    }

    @Override
    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromRM(graphData);
        logger.debugInfo("RM2SG", System.currentTimeMillis() - begin);
        graphData = null;
        super.forceGC();
        begin = System.currentTimeMillis();
        Graph graph = ConverterUtils.buildTinkerPopGraphFromSG(ogDataset);
        logger.debugInfo("SG2LPG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        try {
            DataWriteUtils.writeLPGToCSV(graph, CommonConstant.RM_TO_LPG_RESULT_BASE_URL + "sg.csv");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.debugInfo("WRITE TO CSV", System.currentTimeMillis() - begin);
    }

    @Override
    protected String loggerName() {
        return "FIG13-RDF2LPG";
    }
}
