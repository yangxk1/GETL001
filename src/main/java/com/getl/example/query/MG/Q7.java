package com.getl.example.query.MG;

import com.getl.example.utils.RandomWalk;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.MG.MGraph;
import com.getl.converter.mg.PGMapperR4j;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

/**
 * MG 对照实验 Q7: 随机游走推荐
 */
public class Q7 extends AbstractMGQuery {
    public static void main(String[] args) { new Q7().accept(); }

    @Override
    protected void run() {
        try {
            logger.debugInfo("BEGIN MG Q7");
            loadSourcePG();
            convertPGToMG();
            preparePGForQuery();
            LPGGraph lpgGraph = wrapAsLPG(pgGraphForQuery);
            long begin = System.currentTimeMillis();
            RandomWalk randomWalk = new RandomWalk(lpgGraph);
            LPGGraph resultGraph = new LPGGraph();
            List<List<Vertex>> paths = randomWalk.forward(3);
            logger.debugInfo("random walk end", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            for (List<Vertex> path : paths) {
                if (path.size() < 3) { continue; }
                Vertex v1 = path.get(0); Vertex e = path.get(1); Vertex v2 = path.get(2);
                LPGVertex n1V = resultGraph.getOrCreateVertex(v1.id(), v1.label(), v1.properties());
                LPGVertex n2V = resultGraph.getOrCreateVertex(v2.id(), v2.label(), v2.properties());
                LPGEdge edge = new LPGEdge(resultGraph, n1V, n2V, "recommend");
                edge.addPropertyValue("post", e.id());
            }
            logger.debugInfo("collect recommend edges end", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            MGraph resultMG = new MGraph(); new PGMapperR4j(resultMG).addPGToMG(resultGraph); this.mGraph = resultMG;
            logger.debugInfo("Result LPG -> MG end", (System.currentTimeMillis() - begin));
            exportMGToRDF();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Override
    protected String getLoggerName() { return "MG_QUERY_7"; }
}
