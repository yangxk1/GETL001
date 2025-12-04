package com.getl.example.test;

import com.getl.constant.CommonConstant;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.ConverterUtils;
import com.getl.example.Runnable;
import com.getl.model.MG.MGraph;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;
import org.eclipse.rdf4j.model.Model;

import java.io.File;
import java.io.FileNotFoundException;

public class RDFTest extends Runnable {
    public static void main(String[] args) {
        new RDFTest().accept();
    }

    private Model graphData;

    @Override
    protected void run() {
        loadData();
        System.out.println("================================");
        UG();
        MG();
        SG();
        System.out.println("================================");
        UG();
        MG();
        SG();
    }

    private void loadData() {
        com.getl.Graph graph = new com.getl.Graph();
        String RDF_URL = CommonConstant.RDF_FILES_BASE_URL;
        File resource = new File(RDF_URL);
        try {
            System.out.println("File: " + RDF_URL);
            graph.readRDFFile(RdfDataFormat.TURTLE, resource);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.graphData = graph.getRdfModel();
    }

    private void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGGraphFromRDF(graphData);
        logger.debugInfo("RDF2UG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        Model graph = ConverterUtils.buildRDFGraphFromUG(unifiedGraph);
        logger.debugInfo("UG2RDF", System.currentTimeMillis() - begin);
    }

    private void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGGraphFromRDF(graphData);
        logger.debugInfo("RDF2MG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        Model graph = ConverterUtils.buildRDFGraphFromMG(mGraph);
        logger.debugInfo("MG2RDF", System.currentTimeMillis() - begin);
    }

    private void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromRDF(graphData);
        logger.debugInfo("RDF2SG", System.currentTimeMillis() - begin);
        begin = System.currentTimeMillis();
        Model graph = ConverterUtils.buildRDFGraphFromSG(ogDataset);
        logger.debugInfo("SG2RDF", System.currentTimeMillis() - begin);
    }

    @Override
    protected String loggerName() {
        return "PropertyGraphTest";
    }
}
