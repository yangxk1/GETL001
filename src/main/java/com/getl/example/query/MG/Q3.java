package com.getl.example.query.MG;

import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.MG.MGraph;
import com.getl.converter.mg.PGMapperR4j;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Map;

/**
 * MG 对照实验 Q3: 度数过滤 + 边收集
 */
public class Q3 extends AbstractMGQuery {
    public static void main(String[] args) {
        new Q3().accept();
    }

    @Override
    protected void run() {
        try {
            logger.debugInfo("BEGIN MG Q3");
            loadSourcePG();
            convertPGToMG();
            preparePGForQuery();
            LPGGraph lpgGraph = wrapAsLPG(pgGraphForQuery);
            long begin = System.currentTimeMillis();
            lpgGraph.traversal().V().property("_degree", __.bothE().count()).toList();
            List<Map<String, Object>> results = lpgGraph.traversal().V()
                    .has("_degree", P.gt(20)).as("n1")
                    .outE().as("e1")
                    .inV().has("_degree", P.gt(20)).as("n2")
                    .select("n1", "e1", "n2")
                    .dedup("e1")
                    .toList();
            logger.debugInfo("query end", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            LPGGraph resultGraph = new LPGGraph();
            for (Map<String, Object> result : results) {
                Vertex n1 = (Vertex) result.get("n1");
                Edge e1 = (Edge) result.get("e1");
                Vertex n2 = (Vertex) result.get("n2");
                LPGVertex n1V = resultGraph.getOrCreateVertex(n1.id(), n1.label(), n1.properties());
                LPGVertex n2V = resultGraph.getOrCreateVertex(n2.id(), n2.label(), n2.properties());
                LPGEdge edge = new LPGEdge(resultGraph, n1V, n2V, e1.label());
                edge.setId(e1.id());
            }
            logger.debugInfo("collect to lpg end", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            MGraph resultMG = new MGraph();
            new PGMapperR4j(resultMG).addPGToMG(resultGraph);
            this.mGraph = resultMG;
            logger.debugInfo("Result LPG -> MG end", (System.currentTimeMillis() - begin));
            exportMGToRDF();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getLoggerName() {
        return "MG_QUERY_3";
    }
}
