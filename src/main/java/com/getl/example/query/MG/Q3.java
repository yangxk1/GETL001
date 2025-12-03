package com.getl.example.query.MG;

import com.getl.Graph;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.mg.PGMapperI;
import com.getl.converter.mg.PGMapperR4j;
import com.getl.converter.mg.RDFMapper;
import com.getl.example.Runnable;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.MG.MGraph;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

import static com.getl.constant.CommonConstant.RDF_FILES_BASE_URL;

public class Q3 extends Runnable {

    public static void main(String[] args) {
        new Q3().accept();
    }

    @Override
    protected void run() {
        try {
            logger.debugInfo("BEGIN TO TEST Q3");
            String RDF_URL = RDF_FILES_BASE_URL;
            Graph graph = new Graph();
            long begin = System.currentTimeMillis();
            File resource = new File(RDF_URL);
            try {
                System.out.println("File: " + RDF_URL);
                graph.readRDFFile(RdfDataFormat.TURTLE, resource);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
            logger.debugInfo("READ RDF END ", (System.currentTimeMillis() - begin));

            begin = System.currentTimeMillis();
            RDFMapper rdfMapper = new RDFMapper(new MGraph());
            rdfMapper.addRDFModelToMG(graph.getRdfModel());
            MGraph mGraph = rdfMapper.getMGraph();
            logger.debugInfo("RDF 2 MG END ", (System.currentTimeMillis() - begin));

            begin = System.currentTimeMillis();
            graph = null;
            rdfMapper = null;
            Runtime.getRuntime().gc();
            logger.debugInfo("GC" , (System.currentTimeMillis() - begin));

            begin = System.currentTimeMillis();
            PGMapperI pgMapper = new PGMapperR4j(mGraph);
            org.apache.tinkerpop.gremlin.structure.Graph lpgGraph = pgMapper.createGraphFromMG();
            logger.debugInfo("graph customization 2 LPG end ", System.currentTimeMillis() - begin);
            System.out.println("lpg vertex count: " + lpgGraph.traversal().V().count().next());
            System.out.println("lpg edge count: " + lpgGraph.traversal().E().count().next());

            begin = System.currentTimeMillis();
            mGraph = null;
            pgMapper = null;
            Runtime.getRuntime().gc();
            logger.debugInfo("GC" , (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
// Q3: Select vertices with a degree greater than 5 and the edges between them
            GraphTraversalSource g = lpgGraph.traversal();// Initialize your GraphTraversalSource
            g.V()
                    .property("_degree", __.bothE().count()).toList();
            List<Map<String, Object>> results = lpgGraph.traversal().V()
                    .has("_degree", P.gt(20)).as("n1")
                    .outE().as("e1")
                    .inV().has("_degree", P.gt(20)).as("n2")
                    .select("n1", "e1", "n2")
                    .dedup("e1")
                    .toList();
            logger.debugInfo("query end " , (System.currentTimeMillis() - begin));

            begin = System.currentTimeMillis();
            //  System.out.println(results.size());
            LPGGraph resultGraph = new LPGGraph();
            for (Map<String, Object> result : results) {
                Vertex n1 = (Vertex) result.get("n1");
                Edge e1 = (Edge) result.get("e1");
                Vertex n2 = (Vertex) result.get("n2");
                LPGVertex n1V = resultGraph.getOrCreateVertex(n1.id(), n1.label(), n1.properties());
                LPGVertex n2V = resultGraph.getOrCreateVertex(n2.id(), n2.label(), n2.properties());
                LPGEdge lpgEdge = new LPGEdge(resultGraph, n1V, n2V, e1.label());
                lpgEdge.setId(e1.id());
            }
            logger.debugInfo("collect to lpg end " , (System.currentTimeMillis() - begin));

            lpgGraph = null;
            Runtime.getRuntime().gc();
            logger.debugInfo("GC" , (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            mGraph = new MGraph();
            pgMapper = new PGMapperR4j(mGraph);
            pgMapper.addPGToMG(resultGraph);
            logger.debugInfo("PG2MG end ", (System.currentTimeMillis() - begin));
            lpgGraph = pgMapper.createGraphFromMG();
            logger.debugInfo("result MG 2 LPG end " , (System.currentTimeMillis() - begin));
            System.out.println("lpg vertex count: " + lpgGraph.traversal().V().count().next());
            System.out.println("lpg edge count: " + lpgGraph.traversal().E().count().next());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger("MG QUERY 3");
    }
}
