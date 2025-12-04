package com.getl.experiment.RuntimeOfModelConversion;

import com.getl.constant.CommonConstant;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.ConverterUtils;
import com.getl.example.Runnable;
import com.getl.experiment.DataLoadUtils;
import com.getl.experiment.ExperimentRunner;
import com.getl.model.MG.MGraph;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;
import org.eclipse.rdf4j.model.Model;

import java.io.File;
import java.io.FileNotFoundException;

public class RDF extends ExperimentRunner {
    public static void main(String[] args) {
        new RDF().accept();
    }

    private Model graphData;

    @Override
    protected void loadData() {
        this.graphData = DataLoadUtils.loadRDFModel();
    }

    @Override
    protected void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGGraphFromRDF(graphData);
        logger.debugInfo("RDF2UG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        Model graph = ConverterUtils.buildRDFGraphFromUG(unifiedGraph);
        logger.debugInfo("UG2RDF", System.currentTimeMillis() - begin);
    }

    @Override
    protected void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGGraphFromRDF(graphData);
        logger.debugInfo("RDF2MG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        Model graph = ConverterUtils.buildRDFGraphFromMG(mGraph);
        logger.debugInfo("MG2RDF", System.currentTimeMillis() - begin);
    }

    @Override
    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromRDF(graphData);
        logger.debugInfo("RDF2SG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        Model graph = ConverterUtils.buildRDFGraphFromSG(ogDataset);
        logger.debugInfo("SG2RDF", System.currentTimeMillis() - begin);
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger("FIG13-RDF");
    }
}
