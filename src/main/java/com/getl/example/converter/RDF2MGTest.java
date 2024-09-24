package com.getl.example.converter;

import com.getl.Graph;
import com.getl.constant.CommonConstant;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.mg.RDFMapper;
import com.getl.model.MG.MGraph;
import org.eclipse.rdf4j.model.Model;

import java.io.File;
import java.io.FileNotFoundException;


public class RDF2MGTest {
    public static void main(String[] args) {
        String RDF_URL = CommonConstant.RDF_FILES_BASE_URL;
        System.out.println("BEGIN TO TEST RDF 2 MG");
        System.out.println(System.currentTimeMillis());
        long begin = System.currentTimeMillis();
        Graph graph = new Graph();
        File resource = new File(RDF_URL);
        try {
            System.out.println("File: " + RDF_URL);
            graph.readRDFFile(RdfDataFormat.TURTLE, resource);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        System.out.println("READ RDF END " + (System.currentTimeMillis() - begin));
        System.out.println(System.currentTimeMillis());
        begin = System.currentTimeMillis();
        RDFMapper RDFMapper = new RDFMapper(new MGraph());
        RDFMapper.addRDFModelToMG(graph.getRdfModel());
        System.out.println("RDF 2 MG END " + (System.currentTimeMillis() - begin));
        System.out.println(System.currentTimeMillis());
        begin = System.currentTimeMillis();
        graph.setRdfModel(null);
        graph = null;
        Model rdfModelFromMG = RDFMapper.createRDFModelFromMG();
        System.out.println("URG 2 RDF END " + (System.currentTimeMillis() - begin));
        System.out.println(System.currentTimeMillis());
        System.out.println("RDF SIZE : " + rdfModelFromMG.size());
        System.out.println(System.currentTimeMillis());
    }
}
