package com.getl.example.otherModelConverter;

import com.getl.Graph;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.mg.PGMapperI;
import com.getl.converter.mg.PGMapperR4j;
import com.getl.converter.mg.RDFMapper;
import com.getl.model.MG.MGraph;
import com.getl.util.DebugUtil;

import java.io.File;
import java.io.FileNotFoundException;

import static com.getl.constant.CommonConstant.RDF_FILES_BASE_URL;

public class MGRDF2PG {
    public static void main(String[] args) {
        String RDF_URL = RDF_FILES_BASE_URL;
        DebugUtil.DebugInfo("BEGIN TO TEST RDF2PG by MG");
        long begin = System.currentTimeMillis();
        Graph graph = new Graph();
        File resource = new File(RDF_URL);
        try {
            System.out.println("File: " + RDF_URL);
            graph.readRDFFile(RdfDataFormat.TURTLE, resource);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        DebugUtil.DebugInfo("READ RDF END " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        RDFMapper RDFMapper = new RDFMapper(new MGraph());
        RDFMapper.addRDFModelToMG(graph.getRdfModel());
        DebugUtil.DebugInfo("RDF 2 MG END " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        PGMapperI pgMapper = new PGMapperR4j(RDFMapper.getMGraph());
        org.apache.tinkerpop.gremlin.structure.Graph resultGraph = pgMapper.createGraphFromMG();
        System.out.println("MG2PG END " + (System.currentTimeMillis() - begin));
        System.out.println("Vertices count: " + resultGraph.traversal().V().toList().size());
    }
}
