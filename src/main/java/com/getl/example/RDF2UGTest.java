package com.getl.example;

import com.getl.Graph;
import com.getl.constant.CommonConstant;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.PropertiesGraphConfig;
import com.getl.converter.RDFConverter;
import com.getl.util.DebugUtil;

import java.io.File;
import java.io.FileNotFoundException;


public class RDF2UGTest {
    public static void main(String[] args) {
        String RDF_URL = CommonConstant.RDF_FILES_BASE_URL;
        DebugUtil.DebugInfo("BEGIN TO TEST RDF 2 UGM");
        Graph graph = new Graph();
        long begin = System.currentTimeMillis();
        PropertiesGraphConfig.PropertiesGraphConfigRegister register = new PropertiesGraphConfig.PropertiesGraphConfigRegister();
        PropertiesGraphConfig defaultConfig = register.registerDefaultConfig().addEdgeNamespaceList("https://www.w3schools.com/rdf");
        graph.setDefaultConfig(defaultConfig);
        File resource = new File(RDF_URL);
        try {
            System.out.println("read " + RDF_URL);
            graph.readRDFFile(RdfDataFormat.TURTLE, resource);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        DebugUtil.DebugInfo("READ RDF END " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        graph.handleRDFModel();
        DebugUtil.DebugInfo("RDF 2 URG END " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        graph.setRdfModel(null);
        graph.setRdfConverter(new RDFConverter(graph.getUnifiedGraph()));
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        graph.setRdfModel(null);
        graph.refreshRDF();
        DebugUtil.DebugInfo("URG 2 RDF END " + (System.currentTimeMillis() - begin));
        System.out.println(System.currentTimeMillis());
        System.out.println("RDF SIZE : " + graph.getRdfModel().size());
        System.out.println(System.currentTimeMillis());
    }
}
